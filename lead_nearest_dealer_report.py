#!/usr/bin/env python3
"""
Assign each lead to its *nearest* dealer (and optionally only keep leads within a radius),
then export an Excel report.

Why this is fast:
- Geocodes each unique (city, province/state, country) at most once using geo_city_cache.json
- Computes nearest dealer via pure distance math (no extra geocoding inside the dealer loop)

Inputs:
- leads_export.json from your existing exporter/GUI
- Dealers from odoo_connector.DEALER_LOCATIONS (must have Latitude/Longitude)

Outputs:
- Excel workbook with:
  - "Summary": counts of assigned leads per dealer (within radius filter)
  - "Assigned": one row per lead with nearest dealer + distance_km

Notes:
- If a lead has no city/prov (or geocoding fails), it's skipped (reported in console).
- This assigns *one* dealer per lead (the closest). It does NOT list "all dealers within 50 km".

To run this use python3 lead_nearest_dealer_report.py --leads leads_export.json --radius-km 50 --out lead_nearest_50km.xlsx

"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import Workbook

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from odoo_connector import DEALER_LOCATIONS, haversine_distance

GEO_CACHE_PATH = Path("geo_city_cache.json")
_geolocator = Nominatim(user_agent="WavcorLeadNearestDealer/1.0")

DEFAULT_RADIUS_KM = 50.0

LEAD_COLUMNS = [
    "id",
    "name",
    "type",
    "stage_name",
    "contact_name",
    "partner_name",
    "email_from",
    "phone",
    "mobile",
    "city",
    "province_state_code",
    "province_state_name",
    "province",
    "zip",
    "country_name",
    "country",
    "create_date",
    "write_date",
    "user_name",
    "team_name",
]


def load_leads_export(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unrecognized leads JSON format: {path}")


def _geo_key(city: str, prov: str, country: str) -> str:
    return f"{city.strip().lower()}|{prov.strip().lower()}|{country.strip().lower()}"


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
                if isinstance(v, (list, tuple)) and len(v) == 2 and v[0] is not None and v[1] is not None:
                    clean[k] = (float(v[0]), float(v[1]))
            except Exception:
                continue
    return clean


def _save_geo_cache(cache: Dict[str, Tuple[float, float]]) -> None:
    GEO_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


def geocode_city_prov(
    city: str,
    prov: str,
    country: str,
    cache: Dict[str, Tuple[float, float]],
    attempts: int = 3,
    sleep_s: float = 1.05,
) -> Optional[Tuple[float, float]]:
    """
    Geocode city/prov/country, caching results. Returns (lat, lon) or None.
    Polite throttling for Nominatim via sleep_s between successful calls.
    """
    if not city or not prov:
        return None

    key = _geo_key(city, prov, country)
    if key in cache:
        return cache[key]

    query = f"{city}, {prov}, {country}"
    for i in range(1, attempts + 1):
        try:
            loc = _geolocator.geocode(query, timeout=10)
            if loc:
                cache[key] = (loc.latitude, loc.longitude)
                # polite throttle (only when we actually called out)
                time.sleep(sleep_s)
                return cache[key]
            return None
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(1.5 * i)
        except Exception:
            return None
    return None


def _dealer_name(d: Dict[str, Any]) -> str:
    return str(d.get("Location", "")).strip()


def _prepare_dealers() -> List[Tuple[str, float, float]]:
    dealers: List[Tuple[str, float, float]] = []
    for d in DEALER_LOCATIONS:
        name = _dealer_name(d)
        if not name:
            continue
        try:
            lat = float(d["Latitude"])
            lon = float(d["Longitude"])
        except Exception:
            continue
        dealers.append((name, lat, lon))
    if not dealers:
        raise SystemExit("No valid dealers with Latitude/Longitude found in DEALER_LOCATIONS.")
    return dealers


def _lead_city_prov_country(lead: Dict[str, Any], default_country: str = "Canada") -> Tuple[str, str, str]:
    city = (lead.get("city") or "").strip()
    prov = (
        (lead.get("province_state_code") or "")
        or (lead.get("province_state_name") or "")
        or (lead.get("province") or "")
    ).strip()
    country = (lead.get("country_name") or lead.get("country") or default_country).strip() or default_country
    return city, prov, country


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leads", required=True, help="Path to leads_export.json")
    ap.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM, help="Only keep leads within this distance of their nearest dealer")
    ap.add_argument("--out", default="lead_nearest_dealer.xlsx", help="Output .xlsx filename")
    ap.add_argument("--default-country", default="Canada", help="Fallback country name (default: Canada)")
    args = ap.parse_args()

    leads_path = Path(args.leads).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    print(f"Processing lead nearest dealer report with args: {args}")

    leads = load_leads_export(leads_path)
    dealers = _prepare_dealers()

    # 1) Resolve coordinates for leads (cached by unique city/prov/country)
    cache = _load_geo_cache()
    cache_dirty = False

    resolved: List[Tuple[Dict[str, Any], float, float]] = []
    skipped_missing = 0
    skipped_geocode = 0

    # Build a set of unique keys first to minimize geocode calls
    print(f"Building unique keys for {len(leads)} leads...")
    unique_keys: Dict[str, Tuple[str, str, str]] = {}
    for lead in leads:
        city, prov, country = _lead_city_prov_country(lead, default_country=args.default_country)
        if not city or not prov:
            continue
        k = _geo_key(city, prov, country)
        if k not in unique_keys:
            unique_keys[k] = (city, prov, country)

    # Geocode only keys not already in cache
    print(f"Found {len(unique_keys)} unique city/prov/country combinations.")
    keys_to_geocode = [(k, *unique_keys[k]) for k in unique_keys.keys() if k not in cache]
    if keys_to_geocode:
        print(f"Geocoding {len(keys_to_geocode)} unique city/province combinations (cached in {GEO_CACHE_PATH})...")
    for idx, (k, city, prov, country) in enumerate(keys_to_geocode, start=1):
        coords = geocode_city_prov(city, prov, country, cache)
        if coords:
            cache_dirty = True
        if idx % 25 == 0:
            print(f"  Geocoded {idx}/{len(keys_to_geocode)}...")

    if cache_dirty:
        _save_geo_cache(cache)

    # Now resolve each lead quickly from cache
    print(f"Resolving coordinates for {len(leads)} leads from cache...")
    for lead in leads:
        city, prov, country = _lead_city_prov_country(lead, default_country=args.default_country)
        if not city or not prov:
            skipped_missing += 1
            continue
        k = _geo_key(city, prov, country)
        coords = cache.get(k)
        if not coords:
            skipped_geocode += 1
            continue
        resolved.append((lead, coords[0], coords[1]))

    print(f"Resolved coords for {len(resolved)}/{len(leads)} leads. Skipped missing city/prov: {skipped_missing}. Skipped geocode: {skipped_geocode}.")

    # 2) For each lead, find nearest dealer
    print(f"Finding nearest dealer for {len(resolved)} leads...")
    assigned_rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}

    radius_km = float(args.radius_km)

    for i, (lead, lat, lon) in enumerate(resolved, start=1):
        best_name = None
        best_dist = 1e18
        for name, dlat, dlon in dealers:
            dist = haversine_distance(dlat, dlon, lat, lon)
            if dist < best_dist:
                best_dist = dist
                best_name = name

        if best_name is None:
            continue

        if best_dist <= radius_km:
            out = dict(lead)
            out["nearest_dealer"] = best_name
            out["distance_km"] = round(best_dist, 1)
            assigned_rows.append(out)
            counts[best_name] = counts.get(best_name, 0) + 1

        if i % 250 == 0:
            print(f"Processed {i}/{len(resolved)} leads...")

    # 3) Write Excel
    print(f"Writing Excel report to: {out_path}")
    wb = Workbook()
    ws_summary = wb.active
    ws_summary.title = "Summary"
    ws_assigned = wb.create_sheet("Assigned")

    ws_summary.append(["Dealer", f"Assigned leads within {radius_km:.1f} km"])
    for dealer_name in sorted(counts.keys()):
        ws_summary.append([dealer_name, counts[dealer_name]])

    ws_assigned.append(["nearest_dealer", "distance_km", *LEAD_COLUMNS])
    for row in assigned_rows:
        ws_assigned.append([row.get("nearest_dealer", ""), row.get("distance_km", ""), *[row.get(c, "") if not isinstance(row.get(c, ""), (dict, list)) else str(row.get(c, "")) for c in LEAD_COLUMNS]])

    ws_summary.freeze_panes = "A2"
    ws_assigned.freeze_panes = "A2"

    wb.save(out_path)
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
