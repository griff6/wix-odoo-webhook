from flask import Flask, request, jsonify
import json, traceback, time
import re
import os
from pathlib import Path
from email.utils import parseaddr
from datetime import datetime, timezone, timedelta
from geopy.geocoders import ArcGIS, Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from datetime import datetime, timedelta
from odoo_connector import (
    create_odoo_contact, update_odoo_contact, find_existing_contact,
    create_odoo_opportunity, connect_odoo, get_or_create_opportunity_tags,
    find_odoo_user_id, get_model_id, add_follower_to_lead,
    find_closest_dealer, find_existing_opportunity, update_odoo_opportunity,
    post_internal_note_to_opportunity, ODOO_URL, normalize_state, schedule_activity_for_lead,
    set_dealer_property_on_lead, DEALER_LOCATIONS, haversine_distance, CANONICAL_CODES,
)

app = Flask(__name__)
geolocator = Nominatim(user_agent="WavcorWebhook")
arcgis_geolocator = ArcGIS(timeout=10)
DEALER_LOOKUP_API_KEY = (os.getenv("DEALER_LOOKUP_API_KEY") or "").strip()
GEO_CACHE_PATH = Path("geo_city_cache.json")
BLOCKED_EMAIL_DOMAINS_PATH = Path("blocked_email_domains.txt")
BLOCKED_EMAIL_DOMAINS_ENV = "BLOCKED_EMAIL_DOMAINS"
GEOCODE_CACHE = {}
LAST_GEOCODE_REQUEST_TS = 0.0
GEOCODE_RATE_LIMIT_SECONDS = 1.2
GEOCODE_429_COOLDOWN_SECONDS = 60.0
GEOCODE_COOLDOWN_UNTIL = 0.0
GEOCODE_PROVINCE_NAMES = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}


def _normalize_email_domain(domain: str) -> str:
    domain = str(domain or "").strip().lower()
    domain = domain.removeprefix("@").rstrip(".")
    return domain


def _load_blocked_email_domains() -> set:
    domains = set()

    env_value = os.getenv(BLOCKED_EMAIL_DOMAINS_ENV, "")
    for domain in env_value.split(","):
        normalized = _normalize_email_domain(domain)
        if normalized:
            domains.add(normalized)

    if BLOCKED_EMAIL_DOMAINS_PATH.exists():
        try:
            for line in BLOCKED_EMAIL_DOMAINS_PATH.read_text(encoding="utf-8").splitlines():
                value = line.split("#", 1)[0].strip()
                normalized = _normalize_email_domain(value)
                if normalized:
                    domains.add(normalized)
        except Exception as e:
            print(f"WARNING: Failed to read blocked email domains: {e}", flush=True)

    return domains


def _email_domain(email_value: str) -> str:
    _, parsed_email = parseaddr(str(email_value or ""))
    if "@" not in parsed_email:
        return ""
    return _normalize_email_domain(parsed_email.rsplit("@", 1)[1])


def _is_blocked_email_domain(email_value: str):
    email_domain = _email_domain(email_value)
    if not email_domain:
        return False, ""

    for blocked_domain in _load_blocked_email_domains():
        if email_domain == blocked_domain or email_domain.endswith(f".{blocked_domain}"):
            return True, blocked_domain

    return False, ""


def _blocked_domain_result(data: dict, form_name: str):
    email = data.get("Email") or ""
    is_blocked, blocked_domain = _is_blocked_email_domain(email)
    if not is_blocked:
        return None

    print(
        f"🚫 Blocked {form_name} submission from email domain '{blocked_domain}': {email}",
        flush=True,
    )
    return {
        "status": "blocked",
        "form": form_name,
        "reason": "Blocked email domain",
        "blocked_domain": blocked_domain,
    }


def _geo_key(city: str, prov: str, country: str = "Canada") -> str:
    return f"{city.strip().lower()}|{prov.strip().lower()}|{country.strip().lower()}"


def _normalize_city_text(city: str) -> str:
    text = re.sub(r"[^\w\s-]", " ", str(city or "").strip().lower())
    text = re.sub(r"[\s_-]+", " ", text).strip()
    return text


def _compact_city_text(city: str) -> str:
    return _normalize_city_text(city).replace(" ", "")


def _geo_cache_keys(city: str, province_state: str, country: str = "Canada"):
    province_raw = str(province_state or "").strip()
    province_norm = normalize_state(province_raw)
    province_for_geocode = GEOCODE_PROVINCE_NAMES.get(province_norm, province_raw)
    city_variants = {
        str(city or "").strip(),
        _normalize_city_text(city),
        _compact_city_text(city),
    }
    prov_variants = {
        province_raw,
        province_norm,
        province_for_geocode,
        f"{province_for_geocode} CA",
    }
    return {
        _geo_key(city_variant, prov_variant, country)
        for city_variant in city_variants
        for prov_variant in prov_variants
        if city_variant and prov_variant
    }


def _load_geo_cache() -> dict:
    if not GEO_CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(GEO_CACHE_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_geo_cache(cache: dict) -> None:
    try:
        GEO_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"WARNING: Failed to save geo cache: {e}", flush=True)


def _get_cached_coords(city: str, province_state: str, country: str = "Canada"):
    cache = GEOCODE_CACHE
    for key in _geo_cache_keys(city, province_state, country):
        val = cache.get(key)
        if isinstance(val, (list, tuple)) and len(val) == 2:
            try:
                return float(val[0]), float(val[1])
            except Exception:
                continue

    target_city_norm = _normalize_city_text(city)
    target_city_compact = _compact_city_text(city)
    target_prov_norm = normalize_state(str(province_state or "").strip())
    for key, val in cache.items():
        if not (isinstance(val, (list, tuple)) and len(val) == 2):
            continue
        try:
            cached_city, cached_prov, cached_country = key.split("|", 2)
        except ValueError:
            continue
        if cached_country.strip().lower() != str(country or "").strip().lower():
            continue
        cached_city_norm = _normalize_city_text(cached_city)
        cached_city_compact = _compact_city_text(cached_city)
        cached_prov_norm = normalize_state(cached_prov)
        if (
            cached_prov_norm == target_prov_norm
            and (
                cached_city_norm == target_city_norm
                or cached_city_compact == target_city_compact
            )
        ):
            try:
                coords = (float(val[0]), float(val[1]))
                _store_cached_coords(city, province_state, country, coords)
                return coords
            except Exception:
                continue
    return None


def _store_cached_coords(city: str, province_state: str, country: str, coords) -> None:
    for key in _geo_cache_keys(city, province_state, country):
        GEOCODE_CACHE[key] = coords
    _save_geo_cache(GEOCODE_CACHE)


def _extract_location_coords(location):
    if not location:
        return None
    try:
        return float(location.latitude), float(location.longitude)
    except Exception:
        return None


def _try_arcgis_geocode(full_address: str, city: str, province_state: str, country: str):
    try:
        print(f"DEBUG: ArcGIS geocoding '{full_address}'", flush=True)
        location = arcgis_geolocator.geocode(full_address, timeout=10)
        coords = _extract_location_coords(location)
        if coords:
            print(f"DEBUG: ArcGIS success → {coords[0]}, {coords[1]}", flush=True)
            _store_cached_coords(city, province_state, country, coords)
            return coords
        print(f"WARNING: ArcGIS could not geocode '{full_address}'", flush=True)
        return None, None
    except Exception as e:
        print(f"ERROR: ArcGIS geocoding error: {e}", flush=True)
        return None, None


GEOCODE_CACHE = _load_geo_cache()


def _nearest_dealer_by_distance(customer_lat, customer_lon):
    """Fallback for public lookup page when routing API is unavailable."""
    best = None
    for dealer in DEALER_LOCATIONS:
        dlat = dealer.get("Latitude")
        dlon = dealer.get("Longitude")
        if dlat is None or dlon is None:
            continue
        km = haversine_distance(float(customer_lat), float(customer_lon), float(dlat), float(dlon))
        if best is None or km < best[0]:
            best = (km, dealer)
    if not best:
        return None
    km, dealer = best
    result = dict(dealer)
    result["Distance_km"] = round(km, 2)
    result["Drive_time_hr"] = round(km / 80.0, 2)
    result["route_mode"] = "distance_fallback"
    return result


def _set_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return resp


@app.after_request
def _after_request(resp):
    return _set_cors_headers(resp)

# --------------------------------------------------------------------
# 1️⃣  Flask entrypoint with duplicate protection
# --------------------------------------------------------------------

# Store submissionId + timestamp (instead of plain set)
processed_submissions = {}


@app.route("/nearest_dealer", methods=["POST", "OPTIONS"])
def nearest_dealer():
    """
    Public lookup endpoint for Wix dealer-search page.
    Body JSON:
      {"city":"Lumsden","province":"SK"}
    """
    if request.method == "OPTIONS":
        return _set_cors_headers(app.response_class(status=204))

    try:
        if DEALER_LOOKUP_API_KEY:
            sent_key = (request.headers.get("X-API-Key") or "").strip()
            if sent_key != DEALER_LOOKUP_API_KEY:
                return jsonify({"status": "error", "message": "Unauthorized"}), 401

        payload = request.get_json(silent=True) or {}
        city = str(payload.get("city") or payload.get("City") or "").strip()
        province_raw = (
            payload.get("province")
            or payload.get("prov")
            or payload.get("state")
            or payload.get("Prov/State")
            or ""
        )
        province = normalize_state(str(province_raw).strip())

        if not city or not province:
            return jsonify({
                "status": "error",
                "message": "Both city and province/state are required.",
            }), 400
        if province not in CANONICAL_CODES:
            return jsonify({
                "status": "error",
                "message": (
                    f"Invalid province/state value '{province_raw}'. "
                    "Use a 2-letter code like SK, MB, AB."
                ),
            }), 400

        lat, lon = get_lat_lon_from_address(city, province)
        if lat is None or lon is None:
            return jsonify({
                "status": "no_match",
                "message": "Could not geocode location.",
                "city": city,
                "province": province,
            }), 200

        closest = find_closest_dealer(lat, lon)
        if not closest:
            closest = _nearest_dealer_by_distance(lat, lon)
            if not closest:
                return jsonify({
                    "status": "no_match",
                    "message": "No dealer found within configured driving range.",
                    "city": city,
                    "province": province,
                    "latitude": lat,
                    "longitude": lon,
                }), 200

        return jsonify({
            "status": "ok",
            "city": city,
            "province": province,
            "latitude": lat,
            "longitude": lon,
            "dealer": {
                "location": closest.get("Location"),
                "contact": closest.get("Contact"),
                "phone": closest.get("Phone"),
                "email": closest.get("Email"),
                "distance_km": closest.get("Distance_km"),
                "drive_time_hr": closest.get("Drive_time_hr"),
                "route_mode": closest.get("route_mode", "osrm"),
            },
        }), 200
    except Exception as e:
        print(f"ERROR nearest_dealer: {e}", flush=True)
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/wix_form_webhook", methods=["POST"])
def wix_form_webhook():
    """Main webhook entrypoint for Wix forms"""
    print("🔔 Received webhook request", flush=True)
    try:
        payload = request.get_json(force=True)
        #print("✅ Raw incoming JSON:", json.dumps(payload, indent=2), flush=True)

        # --- Extract submissionId for deduplication ---
        submission_id = payload.get("data", {}).get("submissionId")
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=10)

        # 🧹 Clean out old entries (>10 minutes old)
        for sid, ts in list(processed_submissions.items()):
            if ts < cutoff:
                del processed_submissions[sid]

        if submission_id:
            if submission_id in processed_submissions:
                print(f"⚠️ Duplicate submission ignored: {submission_id}", flush=True)
                return jsonify({"status": "duplicate_ignored"}), 200
            processed_submissions[submission_id] = now
        else:
            print("⚠️ No submissionId found — skipping dedup check", flush=True)

        # --- Process the form ---
        return handle_form(payload)

    except Exception as e:
        print(f"❌ Error processing webhook: {e}", flush=True)
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500



# --------------------------------------------------------------------
# 2️⃣  Central form dispatcher
# --------------------------------------------------------------------
def handle_form(payload):
    """Handles routing based on formName field in Wix payload"""
    try:
        data = payload.get("data", {})
        form_name = data.get("formName", "Unknown Form")
        submissions = data.get("submissions", [])
        fields = {item["label"].strip(): item.get("value", "").strip() for item in submissions}

        print(f"🧾 Parsed Form: {form_name}", flush=True)
        print(f"🧩 Fields: {json.dumps(fields, indent=2)}", flush=True)

        # --- Route based on form name ---
        if form_name == "Quote Form":
            result = handle_quote_form(fields)
        elif form_name == "Contact Form":
            result = handle_contact_form(fields)
        elif form_name == "Manhole Quote Form":
            result = handle_manhole_quote_form(fields)
        else:
            print(f"⚠️ Unknown form type: {form_name}", flush=True)
            result = {"status": "ignored", "reason": f"Unhandled form '{form_name}'"}

        return jsonify(result), 200

    except Exception as e:
        print("❌ Exception occurred in handle_form:", flush=True)
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

# --------------------------------------------------------------------
# 2️⃣  Form Handlers
# --------------------------------------------------------------------
def handle_quote_form(fields):
    """Handle 'Quote Form' submissions from Wix"""
    # Step 1: Build base contact and location info
    data = build_common_data(fields)
    blocked_result = _blocked_domain_result(data, "Quote Form")
    if blocked_result:
        return blocked_result

    # Step 2: Extract quote-specific fields
    name = data["Name"]
    email = data["Email"]
    city = data["City"]
    province = data["Prov/State"]

    products = fields.get("What products are you interested in?", "")
    message_from_form = fields.get(
        "Provide any other information that will help us provide a quote.", ""
    )

    # Step 3: Start building formatted HTML message
    message_parts = []

    if message_from_form:
        message_parts.append(message_from_form.replace("\n", "<br>"))

    if products:
        message_parts.append(f"<b>Products Interested In:</b> {products}")

    # Step 4: Combine and assign final HTML message
    data["Message"] = "<br><br>".join([part for part in message_parts if part])

    #print(
    #    f"📋 Quote Form → {name} | {email} | {city}, {province} | Products: {products}",
    #    flush=True,
    #)

    # Step 6: Push to Odoo
    odoo_result = sync_to_odoo(data)

    return {
        "status": "ok",
        "form": "Quote Form",
        "odoo": odoo_result,
    }



def handle_contact_form(fields):
    """Handle a generic contact form"""
    # Step 1: Build base data (contact, city/province, etc.)
    data = build_common_data(fields)
    blocked_result = _blocked_domain_result(data, "Contact Form")
    if blocked_result:
        return blocked_result

    # Step 2: Extract fields that might exist on the contact form
    name = data["Name"]
    email = data["Email"]
    phone = data.get("Phone", "")
    city = data["City"]
    province = data["Prov/State"]

    # Step 3: Extract user-entered message text safely
    message_value = (
        fields.get("Write a message")
        or fields.get("Message")
        or fields.get("Comments")
        or ""
    ).strip()

    # Step 4: Build the message in HTML format
    message_parts = []

    # Add message text
    if message_value:
        message_parts.append(message_value.replace("\n", "<br>"))

    # Step 5: Combine all parts into one clean HTML message
    data["Message"] = "<br><br>".join([part for part in message_parts if part])

    #print(
    #    f"📩 Contact Form → {name} | {email} | {city}, {province}",
    #    flush=True,
    #)

    # Prevent random tag creation from message text
    data["Products Interest"] = []

    # Step 7: Push to Odoo
    odoo_result = sync_to_odoo(data)

    return {
        "status": "ok",
        "form": "Contact Form",
        "odoo": odoo_result,
    }




def handle_manhole_quote_form(fields):
    """Handle the 'Manhole Quote Form' submission."""
    # Step 1: Build base data (contact info, etc.)
    data = build_common_data(fields)
    blocked_result = _blocked_domain_result(data, "Manhole Quote Form")
    if blocked_result:
        return blocked_result

    # Handle typo variations from Wix ("Privince/State")
    data["Prov/State"] = fields.get("Province/State") or fields.get("Privince/State") or ""

    # Explicitly set product interest
    data["Products Interest"] = ["Manhole Aeration"]

    # Step 2: Extract manhole-specific fields
    manhole_style = fields.get("What style of man hole does your hopper have?")
    extra_info = fields.get(
        "Provide any other information that will help us provide a quote.  "
        "If possible provide the manhole dimensions to allow us to quote more accurately.",
        ""
    )

    # Step 3: Build the main message (without dealer info yet)
    message_parts = []

    # Add any message from the form itself (if provided)
    if data.get("Message"):
        message_parts.append(data["Message"])

    # Add formatted manhole quote info
    message_parts.append(
        f"<b>Manhole Style:</b> {manhole_style or 'N/A'}<br>"
        f"<b>Additional Info:</b> {extra_info or 'N/A'}"
    )

    # Step 4: Join all parts with <br><br> spacing for readability
    data["Message"] = "<br><br>".join([part for part in message_parts if part])

    #print(
    #    f"🕳️ Manhole Quote → {data['Name']} | {data['Email']} | "
    #    f"{data['City']}, {data['Prov/State']} | Style: {manhole_style}",
    #    flush=True,
    #)

    # Step 6: Push to Odoo
    odoo_result = sync_to_odoo(data)

    return {
        "status": "ok",
        "form": "Manhole Quote Form",
        "odoo": odoo_result,
    }



# --------------------------------------------------------------------
# 3️⃣  Shared helper: parse Wix fields into normalized dict
# --------------------------------------------------------------------
def format_north_american_phone(phone_value):
    """Format 10-digit or 11-digit (leading 1) NANP numbers with dashes."""
    raw = (phone_value or "").strip()
    if not raw:
        return ""

    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"1-{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
    return raw


def build_common_data(fields):
    """Flatten Wix fields into your normalized data dict"""
    data = {
        "Name": f"{fields.get('First name', '')} {fields.get('Last name', '')}".strip(),
        "Email": fields.get("Email"),
        "Phone": format_north_american_phone(fields.get("Phone")),
        "City": fields.get("City"),
        "Prov/State": normalize_state(fields.get("Province/State", "")),
        "Products Interest": [],
        "Message": fields.get("Provide any other information that will help us provide a quote.", "")
    }

    # --- Products ---
    raw_products = fields.get("What products are you interested in?", "")
    if isinstance(raw_products, str):
        data["Products Interest"] = [item.strip() for item in raw_products.split(",") if item.strip()]
    elif raw_products:
        data["Products Interest"] = list(raw_products)

    # 🔧 Important: Replace after message composition
    data["Message"] = data["Message"].replace("\n", "<br>")

    #print(f"DEBUG: Final Message to send to Odoo:\n{data['Message']}", flush=True)
    return data



# --------------------------------------------------------------------
# 4️⃣  Dealer lookup and geocoding
# --------------------------------------------------------------------
def build_dealer_info(data):
    """Find nearest dealer (driving <=2h) and return formatted string."""
    if not data["City"] or not data["Prov/State"]:
        return ""
    lat, lon = get_lat_lon_from_address(data["City"], data["Prov/State"])
    if lat is None or lon is None:
        return ""
    closest = find_closest_dealer(lat, lon)
    if not closest:
        return ""
    return (
        f"Closest Dealer: {closest['Location']}\n"
        f"Drive Distance: {closest['Distance_km']} km\n"
        f"Drive Time: {closest['Drive_time_hr']} hr"
    )


def get_lat_lon_from_address(city, province_state, country="Canada", attempt=1):
    """Geocode city+province to latitude/longitude, with retries"""
    global LAST_GEOCODE_REQUEST_TS, GEOCODE_COOLDOWN_UNTIL

    province_norm = str(province_state or "").strip().upper()
    province_for_geocode = GEOCODE_PROVINCE_NAMES.get(province_norm, province_state)
    full_address = f"{city}, {province_for_geocode}, {country}"
    cached = _get_cached_coords(city, province_state, country)
    if cached:
        return cached

    now = time.time()
    if now < GEOCODE_COOLDOWN_UNTIL:
        remaining = round(GEOCODE_COOLDOWN_UNTIL - now, 1)
        print(
            f"WARNING: Skipping geocode for '{full_address}' during 429 cooldown ({remaining}s remaining)",
            flush=True,
        )
        return None, None

    wait_s = GEOCODE_RATE_LIMIT_SECONDS - (now - LAST_GEOCODE_REQUEST_TS)
    if wait_s > 0:
        time.sleep(wait_s)

    print(f"DEBUG: Geocoding '{full_address}' (attempt {attempt})", flush=True)
    try:
        LAST_GEOCODE_REQUEST_TS = time.time()
        location = geolocator.geocode(full_address, timeout=10)
        coords = _extract_location_coords(location)
        if coords:
            print(f"DEBUG: Success → {coords[0]}, {coords[1]}", flush=True)
            _store_cached_coords(city, province_state, country, coords)
            return coords
        print(f"WARNING: Could not geocode '{full_address}'", flush=True)
        return _try_arcgis_geocode(full_address, city, province_state, country)
    except GeocoderTimedOut:
        if attempt < 3:
            print("Retrying geocode after timeout...", flush=True)
            time.sleep(2)
            return get_lat_lon_from_address(city, province_state, country, attempt + 1)
        print("ERROR: Geocoding permanently failed after retries", flush=True)
        return _try_arcgis_geocode(full_address, city, province_state, country)
    except Exception as e:
        print(f"ERROR: Geocoding error: {e}", flush=True)
        if "429" in str(e):
            GEOCODE_COOLDOWN_UNTIL = time.time() + GEOCODE_429_COOLDOWN_SECONDS
            print(
                f"WARNING: Entering geocode cooldown for {int(GEOCODE_429_COOLDOWN_SECONDS)}s after 429 response",
                flush=True,
            )
        return _try_arcgis_geocode(full_address, city, province_state, country)


# --------------------------------------------------------------------
# 5️⃣  Central Odoo Sync Logic
# --------------------------------------------------------------------
def sync_to_odoo(data):
    """Core logic for creating/updating contacts and opportunities in Odoo"""
    try:
        uid, models = connect_odoo()
        if not uid:
            print("❌ Odoo connection failed", flush=True)
            return {"status": "error", "message": "Could not connect to Odoo"}

        print(f"🔗 Connected to Odoo as UID {uid}", flush=True)

        # --- Contact handling ---
        existing = find_existing_contact(data)
        if existing:
            print(f"👤 Contact exists: {existing['name']} (ID {existing['id']})", flush=True)
            success = update_odoo_contact(existing['id'], data)
            contact_id = existing['id']
        else:
            contact_id = create_odoo_contact(data)
            print(f"🆕 Created new contact ID {contact_id}", flush=True)

        if not contact_id:
            return {"status": "error", "message": "Contact creation/update failed"}

        # --- Opportunity handling ---
        opportunity_name = data["Name"].strip()
        existing_opp = find_existing_opportunity(opportunity_name)
        opportunity_tag_ids = get_or_create_opportunity_tags(models, uid, data["Products Interest"])
        message_html = data["Message"]

        city = data.get("City") or ""
        prov = data.get("Prov/State") or ""
        closest = None
        geocode_failed = False

        if city and prov:
            lat, lon = get_lat_lon_from_address(city, prov)
            if lat is not None and lon is not None:
                closest = find_closest_dealer(lat, lon)
            else:
                geocode_failed = True

        dealer_note_lines = []
        if closest:
            if closest.get("Contact"):
                dealer_note_lines.append(f"Dealer Contact Name: {closest['Contact']}")
            if closest.get("Phone"):
                dealer_note_lines.append(f"Dealer Contact Phone: {closest['Phone']}")
            if closest.get("Email"):
                dealer_note_lines.append(f"Dealer Contact Email: {closest['Email']}")

        dealer_note_html = (
            "<b>Dealer Contact</b><br>" + "<br>".join(dealer_note_lines)
            if dealer_note_lines else ""
        )
        if message_html and dealer_note_html:
            description_html = f"{message_html}<br><br>{dealer_note_html}"
        elif dealer_note_html:
            description_html = dealer_note_html
        else:
            description_html = message_html

        if existing_opp:
            print(f"📂 Updating existing opportunity {existing_opp['id']}", flush=True)
            update_data = {
                "partner_id": contact_id,
                "description": description_html,
                "tag_ids": [(6, 0, opportunity_tag_ids)] if opportunity_tag_ids else False,
            }
            update_odoo_opportunity(existing_opp["id"], update_data)
            opportunity_id = existing_opp["id"]
        else:
            print(f"➕ Creating new opportunity for {data['Name']}", flush=True)
            opp_data = {
                "name": opportunity_name,
                "partner_id": contact_id,
                "description": description_html,
                "tag_ids": [(6, 0, opportunity_tag_ids)] if opportunity_tag_ids else False,
                "city": data.get("City") or False,
                "Prov/State": data.get("Prov/State") or "",
            }
            #print(f"DEBUG: About to call create_odoo_opportunity with data: {json.dumps(opp_data, indent=2)}", flush=True)
            opportunity_id = create_odoo_opportunity(opp_data)
            #print(f"DEBUG: create_odoo_opportunity() returned: {opportunity_id}", flush=True)            

        if not opportunity_id:
            return {"status": "error", "message": "Opportunity create/update failed"}

        # --- Set Dealer property on the lead/opportunity (driving-distance logic) ---
        if city and prov:
            print(
                f"DEBUG dealer_sync: evaluating dealer for lead {opportunity_id} city='{city}' prov='{prov}'",
                flush=True,
            )
            if closest and closest.get("Location"):
                set_ok = set_dealer_property_on_lead(models, uid, opportunity_id, closest["Location"])
                if set_ok:
                    print(
                        f"🏷️ Set Dealer property on lead {opportunity_id} "
                        f"to closest in-range dealer: {closest['Location']} "
                        f"({closest.get('Distance_km')} km, {closest.get('Drive_time_hr')} hr)",
                        flush=True,
                    )
                else:
                    print(
                        f"⚠️ Could not set Dealer property on lead {opportunity_id} "
                        f"from closest dealer '{closest['Location']}'",
                        flush=True,
                    )
            elif geocode_failed:
                print(
                    f"INFO dealer_sync: geocoding failed for lead {opportunity_id} city='{city}' prov='{prov}'",
                    flush=True,
                )
            else:
                print(
                    f"INFO dealer_sync: no dealer candidate selected for lead {opportunity_id}",
                    flush=True,
                )
        else:
            print(
                f"INFO dealer_sync: missing city/province for lead {opportunity_id}; skipping dealer assignment",
                flush=True,
            )

        # --- Optional: add follow-up activity ---
        jeff_id = find_odoo_user_id(models, uid, "Jeff Buckton")
        if jeff_id:
            add_follower_to_lead(models, uid, opportunity_id, jeff_id)
            #activity_data = {
            #    "res_model": "crm.lead",          # ✅ use model name instead of numeric ID
            #    "res_id": opportunity_id,
            #    "user_id": al_id,
            #    "summary": "Follow up on email",
            #    "date_deadline": datetime.now().strftime("%Y-%m-%d"),  # due now
            #    "note": f"Follow-up for {data['Name']}",
            #}
            #schedule_activity_for_lead(models, uid, activity_data)
            schedule_activity_for_lead(
                models,
                uid,
                opportunity_id,
                jeff_id,
                "Follow up on email",
                f"Follow-up for {data['Name']}",
            )
            print(f"🗓️ Created immediate activity for opportunity {opportunity_id}", flush=True)


        opportunity_url = f"{ODOO_URL}/web#id={opportunity_id}&model=crm.lead"
        #print(f"✅ Odoo sync complete — {opportunity_url}", flush=True)
        return {"status": "ok", "opportunity_url": opportunity_url}

    except Exception as e:
        print(f"❌ Odoo sync exception: {e}", flush=True)
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# --------------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Flask webhook server starting...", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=True)
