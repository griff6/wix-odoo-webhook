from flask import Flask, request, jsonify
import json, traceback, time
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from odoo_connector import (
    create_odoo_contact, update_odoo_contact, find_existing_contact,
    create_odoo_opportunity, connect_odoo, get_or_create_opportunity_tags,
    find_odoo_user_id, create_odoo_activity, get_model_id,
    find_closest_dealer, find_existing_opportunity, update_odoo_opportunity,
    post_internal_note_to_opportunity, ODOO_URL, normalize_state
)

app = Flask(__name__)
geolocator = Nominatim(user_agent="WavcorWebhook")

# --------------------------------------------------------------------
# 1️⃣  Flask entrypoint
# --------------------------------------------------------------------
@app.route("/wix_form_webhook", methods=["POST"])
def handle_form():
    print("🔔 Received webhook request", flush=True)
    try:
        raw_data = request.get_json(force=True)
        print("✅ Raw incoming JSON:", json.dumps(raw_data, indent=2), flush=True)

        data = raw_data.get("data", {})
        form_name = data.get("formName", "Unknown Form")
        submissions = data.get("submissions", [])
        fields = {item["label"].strip(): item.get("value", "").strip() for item in submissions}

        print(f"🧾 Parsed Form: {form_name}", flush=True)
        print(f"🧩 Fields: {json.dumps(fields, indent=2)}", flush=True)

        # Route by form name
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
        print("❌ Exception occurred:", flush=True)
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


# --------------------------------------------------------------------
# 2️⃣  Form Handlers
# --------------------------------------------------------------------
def handle_quote_form(fields):
    """Handle 'Quote Form' submissions from Wix"""
    data = build_common_data(fields)
    print(f"📋 Quote form received: {data['Name']}, {data['Email']}, {data['City']}, {data['Prov/State']}", flush=True)
    odoo_result = sync_to_odoo(data)
    return {"status": "ok", "form": "Quote Form", "odoo": odoo_result}


def handle_contact_form(fields):
    """Handle a generic contact form"""
    data = build_common_data(fields)
    print(f"📩 Contact form from {data['Name']} <{data['Email']}> — {data['Message']}", flush=True)
    odoo_result = sync_to_odoo(data)
    return {"status": "ok", "form": "Contact Form", "odoo": odoo_result}


def handle_manhole_quote_form(fields):
    """Handle a specific 'Manhole Quote Form'"""
    data = build_common_data(fields)
    data["Products Interest"] = ["Manhole Aeration"]
    print(f"🕳️ Manhole quote form received: {data['Name']}, {data['Email']}, {data['City']}, {data['Prov/State']}", flush=True)
    odoo_result = sync_to_odoo(data)
    return {"status": "ok", "form": "Manhole Quote Form", "odoo": odoo_result}


# --------------------------------------------------------------------
# 3️⃣  Shared helper: parse Wix fields into normalized dict
# --------------------------------------------------------------------
def build_common_data(fields):
    """Flatten Wix fields into your normalized data dict"""
    data = {
        "Name": f"{fields.get('First name', '')} {fields.get('Last name', '')}".strip(),
        "Email": fields.get("Email"),
        "Phone": fields.get("Phone"),
        "City": fields.get("City"),
        "Prov/State": normalize_state(fields.get("Province/State", "")),
        "Products Interest": [],
        "Message": fields.get("Provide any other information that will help us provide a quote.", "")
    }

    raw_products = fields.get("What products are you interested in?", "")
    if isinstance(raw_products, str):
        data["Products Interest"] = [item.strip() for item in raw_products.split(",") if item.strip()]
    elif raw_products:
        data["Products Interest"] = list(raw_products)

    # Append closest dealer info
    dealer_info = build_dealer_info(data)
    if dealer_info:
        data["Message"] += f"\n\n--- Closest Dealer Recommendation ---\n{dealer_info}"
    data["Message"] = data["Message"].replace("\n", "<br>")
    return data


# --------------------------------------------------------------------
# 4️⃣  Dealer lookup and geocoding
# --------------------------------------------------------------------
def build_dealer_info(data):
    """Find nearest dealer and return formatted string"""
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
        f"Contact: {closest['Contact']}\n"
        f"Phone: {closest['Phone']}\n"
        f"Distance: {closest['Distance_km']} km"
    )


def get_lat_lon_from_address(city, province_state, country="Canada", attempt=1):
    """Geocode city+province to latitude/longitude, with retries"""
    full_address = f"{city}, {province_state}, {country}"
    print(f"DEBUG: Geocoding {full_address} (attempt {attempt})", flush=True)
    try:
        location = geolocator.geocode(full_address, timeout=10)
        if location:
            print(f"DEBUG: Success → {location.latitude}, {location.longitude}", flush=True)
            return location.latitude, location.longitude
        print(f"WARNING: Could not geocode '{full_address}'", flush=True)
        return None, None
    except GeocoderTimedOut:
        if attempt < 3:
            print("Retrying geocode after timeout...", flush=True)
            time.sleep(2)
            return get_lat_lon_from_address(city, province_state, country, attempt + 1)
        print("ERROR: Geocoding permanently failed after retries", flush=True)
        return None, None
    except Exception as e:
        print(f"ERROR: Geocoding error: {e}", flush=True)
        return None, None


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

        if existing_opp:
            print(f"📂 Updating existing opportunity {existing_opp['id']}", flush=True)
            update_data = {
                "partner_id": contact_id,
                "description": message_html,
                "tag_ids": [(6, 0, opportunity_tag_ids)] if opportunity_tag_ids else False,
            }
            update_odoo_opportunity(existing_opp["id"], update_data)
            opportunity_id = existing_opp["id"]
        else:
            print(f"➕ Creating new opportunity for {data['Name']}", flush=True)
            opp_data = {
                "name": opportunity_name,
                "partner_id": contact_id,
                "description": message_html,
                "tag_ids": [(6, 0, opportunity_tag_ids)] if opportunity_tag_ids else False,
            }
            opportunity_id = create_odoo_opportunity(opp_data)

        if not opportunity_id:
            return {"status": "error", "message": "Opportunity create/update failed"}

        # --- Optional: add follow-up activity ---
        al_id = find_odoo_user_id(models, uid, "Al Baraniuk")
        if al_id:
            activity_data = {
                "res_model": "crm.lead",          # ✅ use model name instead of numeric ID
                "res_id": opportunity_id,
                "user_id": al_id,
                "summary": "Follow up on email",
                "date_deadline": datetime.now().strftime("%Y-%m-%d"),  # due now
                "note": f"Follow-up for {data['Name']}",
            }
            create_odoo_activity(models, uid, activity_data)
            print(f"🗓️ Created immediate activity for opportunity {opportunity_id}", flush=True)


        opportunity_url = f"{ODOO_URL}/web#id={opportunity_id}&model=crm.lead"
        print(f"✅ Odoo sync complete — {opportunity_url}", flush=True)
        return {"status": "ok", "opportunity_url": opportunity_url}

    except Exception as e:
        print(f"❌ Odoo sync exception: {e}", flush=True)
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# --------------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Flask webhook server starting...", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=True)
