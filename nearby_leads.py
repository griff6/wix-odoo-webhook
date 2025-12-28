#!/usr/bin/env python3
"""
nearby_leads.py

List CRM leads within a radius (km) of a selected Co-op / dealer location.

Inputs:
- leads_export.json produced by export_leads_json.py
- Co-op locations from odoo_connector.DEALER_LOCATIONS

Geocoding:
- Uses city + province/state only (as requested)
- Uses Nominatim via geopy (same general approach as webhook_server.py)
- Caches city/province -> lat/lon in geo_city_cache.json for speed and rate-limit friendliness
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from odoo_connector import DEALER_LOCATIONS, haversine_distance  # haversine_distance is already in your repo


# -----------------------------
# Geocode cache
# -----------------------------
GEO_CACHE_PATH = Path("geo_city_cache.json")

def _load_geo_cache() -> Dict[str, Tuple[float, float]]:
    if not GEO_CACHE_PATH.exists():
        return {}

    try:
        raw = json.loads(GEO_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    clean: Dict[str, Tuple[float, float]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                if (
                    isinstance(v, (list, tuple))
                    and len(v) == 2
                    and v[0] is not None
                    and v[1] is not None
                ):
                    lat = float(v[0])
                    lon = float(v[1])
                    clean[k] = (lat, lon)
            except Exception:
                # skip invalid entries
                continue

    # If we removed anything, rewrite the cache so it stays clean
    if clean != raw:
        _save_geo_cache(clean)

    return clean


def _save_geo_cache(cache: Dict[str, Tuple[float, float]]) -> None:
    GEO_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")

def _geo_key(city: str, prov: str, country: str = "Canada") -> str:
    return f"{city.strip().lower()}|{prov.strip().lower()}|{country.strip().lower()}"

_geolocator = Nominatim(user_agent="WavcorLeadRadius/1.0")

def geocode_city_prov(
    city: str,
    prov: str,
    cache: Dict[str, Tuple[float, float]],
    country: str = "Canada",
    attempts: int = 3,
) -> Tuple[Optional[Tuple[float, float]], bool]:
    """
    Returns: (coords, cache_hit)
    """
    if not city or not prov:
        return None, False

    key = _geo_key(city, prov, country)
    if key in cache:
        lat, lon = cache[key]
        return (float(lat), float(lon)), True

    query = f"{city}, {prov}, {country}"

    for i in range(1, attempts + 1):
        try:
            loc = _geolocator.geocode(query, timeout=10)
            if loc:
                cache[key] = (loc.latitude, loc.longitude)
                return (loc.latitude, loc.longitude), False
            return None, False
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(1.5 * i)
        except Exception:
            return None, False

    return None, False



# -----------------------------
# Leads file loading
# -----------------------------
def load_leads_export(path: str) -> List[Dict[str, Any]]:
    """
    Supports either:
    - {"records":[...], ...} (the schema from export_leads_json.py)
    - [...] (a raw list, if you ever output that)
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict) and "records" in data and isinstance(data["records"], list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unrecognized leads JSON format in {path}")


def pick_dealer(query: str) -> Dict[str, Any]:
    """
    Find a dealer by exact or partial match on Location.
    """
    q = query.strip().lower()
    if not q:
        raise ValueError("Dealer name is required.")

    # Exact match
    for d in DEALER_LOCATIONS:
        if str(d.get("Location", "")).strip().lower() == q:
            return d

    # Partial match (first hit)
    hits = []
    for d in DEALER_LOCATIONS:
        name = str(d.get("Location", "")).strip().lower()
        if q in name:
            hits.append(d)

    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        # Provide helpful error with top matches
        names = [h.get("Location") for h in hits[:15]]
        raise ValueError(f"Multiple dealers match '{query}'. Be more specific. Matches: {names}")

    raise ValueError(f"No dealer found matching '{query}'.")


def list_dealers() -> None:
    for d in DEALER_LOCATIONS:
        print(d.get("Location", ""))


# -----------------------------
# Core computation
# -----------------------------
def leads_within_radius_km(
    dealer: Dict[str, Any],
    leads: List[Dict[str, Any]],
    radius_km: float = 100.0,
    default_country: str = "Canada",
) -> List[Dict[str, Any]]:
    dealer_lat = float(dealer["Latitude"])
    dealer_lon = float(dealer["Longitude"])

    cache = _load_geo_cache()

    results: List[Dict[str, Any]] = []
    skipped_missing_addr = 0
    skipped_geocode_fail = 0

    for lead in leads:
        city = (lead.get("city") or "").strip()

        # Your export includes both name and code; use code if present, else name.
        prov = (
            (lead.get("province_state_code") or "")
            or (lead.get("province_state_name") or "")
            or (lead.get("province") or "")
        ).strip()

        country = (lead.get("country_name") or lead.get("country") or default_country).strip() or default_country

        if not city or not prov:
            skipped_missing_addr += 1
            continue

        cache_dirty = False

        key = _geo_key(city, prov, country)

        coords = cache.get(key)
        cache_hit = coords is not None

        if not cache_hit:
            coords, cache_hit = geocode_city_prov(city, prov, cache, country=country)
            if coords is None:
                skipped_geocode_fail += 1
                continue
            cache_dirty = True
        else:
            # coords is already (lat, lon)
            pass


        if coords is None:
            skipped_geocode_fail += 1
            continue

        if not cache_hit:
            cache_dirty = True

        # Explicitly unpack coordinates
        lat, lon = coords

        # Defensive safety check
        if lat is None or lon is None:
            skipped_geocode_fail += 1
            continue

        dist = haversine_distance(dealer_lat, dealer_lon, lat, lon)

        if dist <= radius_km:
            out = dict(lead)
            out["distance_km"] = round(dist, 1)
            results.append(out)

        # Polite throttling ONLY when we just created a new cache entry.
        # A simple approach is: if key not present we pause. We can detect that by checking again.
        # (If you're doing large first-time runs, you may want to increase this.)
        # NOTE: This is conservative; cached lookups won't sleep.
        # We do not know if this call was cached without tracking; simplest is accept occasional sleep
        # only if cache file is being populated heavily by adding a slightly larger sleep for safety.
        # Comment this out if you use a paid geocoder.
        # time.sleep(0.1)

    if cache_dirty:
        _save_geo_cache(cache)

    results.sort(key=lambda x: x.get("distance_km", 1e9))

    print(f"Skipped (missing city/province): {skipped_missing_addr}")
    print(f"Skipped (geocode failed):        {skipped_geocode_fail}")

    return results


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        Path(path).write_text("", encoding="utf-8")
        return

    # Common columns first; then any extras
    preferred = [
        "distance_km",
        "lead_id",
        "name",
        "type",
        "stage_name",
        "company_name",
        "contact_name",
        "city",
        "province_state_code",
        "province_state_name",
        "country_name",
        "email",
        "phone",
        "mobile",
        "created_at",
        "updated_at",
    ]

    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())

    # Keep preferred order, then remaining keys
    fieldnames = [k for k in preferred if k in all_keys] + sorted([k for k in all_keys if k not in preferred])

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    ap = argparse.ArgumentParser(description="List leads within N km of a selected Co-op dealer location.")
    ap.add_argument("--leads", default="leads_export.json", help="Path to leads_export.json (default: leads_export.json)")
    ap.add_argument("--dealer", help="Dealer/Co-op name (matches DEALER_LOCATIONS['Location'])")
    ap.add_argument("--radius", type=float, default=100.0, help="Radius in km (default: 100)")
    ap.add_argument("--limit", type=int, default=200, help="Max rows to print (default: 200)")
    ap.add_argument("--csv", default="", help="Optional: write results to this CSV path")
    ap.add_argument("--list-dealers", action="store_true", help="Print available dealers and exit")
    args = ap.parse_args()

    if args.list_dealers:
        list_dealers()
        return

    if not args.dealer:
        raise SystemExit("ERROR: --dealer is required (or use --list-dealers).")

    dealer = pick_dealer(args.dealer)
    leads = load_leads_export(args.leads)

    print(f"Dealer: {dealer['Location']}  ({dealer['Latitude']}, {dealer['Longitude']})")
    print(f"Radius: {args.radius} km")
    print(f"Leads loaded: {len(leads)}\n")

    matches = leads_within_radius_km(dealer, leads, radius_km=args.radius)

    print(f"\nMatches within {args.radius:.0f} km: {len(matches)}\n")

    for r in matches[: args.limit]:
        phone = (r.get("phone") or "").strip()
        mobile = (r.get("mobile") or "").strip()
        email = (r.get("email") or "").strip()

        # Prefer phone, then mobile (or show both if you want)
        phones = ", ".join([x for x in [phone, mobile] if x])

        print(
            f"{r.get('distance_km', ''):>6} km | "
            f"{(r.get('name') or '')[:45]:<45} | "
            f"{(r.get('city') or '')}, {r.get('province_state_code') or r.get('province_state_name') or ''} | "
            f"{(r.get('stage_name') or '')[:18]:<18} | "
            f"Phone: {phones or '-':<22} | "
            f"Email: {email or '-'}"
        )


    if args.csv:
        write_csv(args.csv, matches)
        print(f"\nWrote CSV: {args.csv}")


if __name__ == "__main__":
    main()
