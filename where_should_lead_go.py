#!/usr/bin/env python3
"""
Find which dealer a lead should be sent to, using existing routing code.

Default query: Vegreville, Alberta, Canada.

Routing order:
1) Use cached city coordinates from geo_city_cache.json when available.
2) Otherwise geocode with Nominatim (if installed/available).
3) Use odoo_connector.find_closest_dealer (OSRM driving <= 2h logic).
4) If OSRM is unavailable, fall back to nearest straight-line dealer.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Tuple

from odoo_connector import (
    DEALER_LOCATIONS,
    MAX_DEALER_DRIVE_HOURS,
    find_closest_dealer,
    haversine_distance,
    normalize_state,
)

GEO_CACHE_PATH = Path("geo_city_cache.json")


def _geo_key(city: str, prov: str, country: str) -> str:
    return f"{city.strip().lower()}|{prov.strip().lower()}|{country.strip().lower()}"


def _load_geo_cache() -> dict:
    if not GEO_CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(GEO_CACHE_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _coords_from_cache(city: str, province: str, country: str) -> Optional[Tuple[float, float]]:
    cache = _load_geo_cache()
    prov = province.strip()
    country_clean = country.strip() or "Canada"

    candidates = [
        _geo_key(city, prov, country_clean),
        _geo_key(city, f"{prov} CA", country_clean),
        _geo_key(city, normalize_state(prov), country_clean),
    ]

    for key in candidates:
        val = cache.get(key)
        if isinstance(val, (list, tuple)) and len(val) == 2:
            try:
                return float(val[0]), float(val[1])
            except Exception:
                continue
    return None


def _coords_from_geocode(city: str, province: str, country: str) -> Optional[Tuple[float, float]]:
    try:
        from geopy.geocoders import Nominatim
    except Exception:
        return None

    geolocator = Nominatim(user_agent="WavcorDealerLookup")
    query = f"{city}, {province}, {country}"
    try:
        loc = geolocator.geocode(query, timeout=10)
        if loc:
            return float(loc.latitude), float(loc.longitude)
    except Exception:
        return None
    return None


def _nearest_dealer_haversine(lat: float, lon: float):
    best = None
    for dealer in DEALER_LOCATIONS:
        try:
            dlat = float(dealer["Latitude"])
            dlon = float(dealer["Longitude"])
        except Exception:
            continue
        km = haversine_distance(lat, lon, dlat, dlon)
        if best is None or km < best[0]:
            best = (km, dealer)
    return best


def main() -> int:
    parser = argparse.ArgumentParser(description="Find where a lead should be sent.")
    parser.add_argument("--city", default="Vegreville", help="Lead city")
    parser.add_argument("--province", default="Alberta", help="Lead province/state")
    parser.add_argument("--country", default="Canada", help="Lead country")
    args = parser.parse_args()

    city = args.city.strip()
    province = args.province.strip()
    country = args.country.strip()

    coords = _coords_from_cache(city, province, country)
    source = "geo_city_cache.json"
    if not coords:
        coords = _coords_from_geocode(city, province, country)
        source = "geopy"

    if not coords:
        print(f"Could not resolve coordinates for: {city}, {province}, {country}")
        return 1

    lat, lon = coords
    print(f"Lead location: {city}, {province}, {country}")
    print(f"Coordinates: {lat:.6f}, {lon:.6f} (from {source})")

    closest = find_closest_dealer(lat, lon, max_drive_hours=MAX_DEALER_DRIVE_HOURS)
    if closest:
        print("\nRouting result (driving distance logic):")
        print(f"Dealer: {closest.get('Location', '')}")
        print(f"Contact: {closest.get('Contact', '')}")
        print(f"Phone: {closest.get('Phone', '')}")
        print(f"Drive distance: {closest.get('Distance_km', '')} km")
        print(f"Drive time: {closest.get('Drive_time_hr', '')} hr")
        return 0

    # Offline/network-failure fallback
    fallback = _nearest_dealer_haversine(lat, lon)
    if fallback:
        km, dealer = fallback
        print("\nRouting result (fallback: straight-line nearest dealer):")
        print("Driving route lookup was unavailable or no dealer was within the driving-hour threshold.")
        print(f"Dealer: {dealer.get('Location', '')}")
        print(f"Contact: {dealer.get('Contact', '')}")
        print(f"Phone: {dealer.get('Phone', '')}")
        print(f"Approx straight-line distance: {km:.2f} km")
        return 0

    print("No dealer could be selected.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
