import json
import time
from pathlib import Path
from typing import Dict, Tuple, Optional, List, Any

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from odoo_connector import DEALER_LOCATIONS, haversine_distance  # already in your codebase :contentReference[oaicite:5]{index=5}

# Persistent cache for city/province -> (lat, lon)
GEO_CACHE_PATH = Path("geo_city_cache.json")
geolocator = Nominatim(user_agent="WavcorLeadRadius")  # similar to webhook_server :contentReference[oaicite:6]{index=6}

def _load_geo_cache() -> Dict[str, Tuple[float, float]]:
    if GEO_CACHE_PATH.exists():
        return json.loads(GEO_CACHE_PATH.read_text())
    return {}

def _save_geo_cache(cache: Dict[str, Tuple[float, float]]) -> None:
    GEO_CACHE_PATH.write_text(json.dumps(cache, indent=2))

def _geo_key(city: str, prov: str, country: str = "Canada") -> str:
    return f"{city.strip().lower()}|{prov.strip().lower()}|{country.strip().lower()}"

def geocode_city_prov(city: str, prov: str, cache: Dict[str, Tuple[float, float]], country: str = "Canada") -> Optional[Tuple[float, float]]:
    if not city or not prov:
        return None

    key = _geo_key(city, prov, country)
    if key in cache:
        lat, lon = cache[key]
        return float(lat), float(lon)

    query = f"{city}, {prov}, {country}"
    for attempt in range(1, 4):
        try:
            loc = geolocator.geocode(query, timeout=10)
            if loc:
                cache[key] = (loc.latitude, loc.longitude)
                return loc.latitude, loc.longitude
            return None
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(1.5 * attempt)
        except Exception:
            return None
    return None

def find_dealer_by_name(dealer_name: str) -> Dict[str, Any]:
    name_norm = dealer_name.strip().lower()
    for d in DEALER_LOCATIONS:
        if d["Location"].strip().lower() == name_norm:
            return d
    # convenience: partial match
    for d in DEALER_LOCATIONS:
        if name_norm in d["Location"].strip().lower():
            return d
    raise ValueError(f"Dealer not found: {dealer_name!r}")

def load_leads_cache(path: str = "leads_cache.json") -> List[Dict[str, Any]]:
    return json.loads(Path(path).read_text())

def leads_within_radius_of_dealer(
    dealer: Dict[str, Any],
    leads: List[Dict[str, Any]],
    radius_km: float = 100.0,
    country_default: str = "Canada"
) -> List[Dict[str, Any]]:
    cache = _load_geo_cache()

    dealer_lat = float(dealer["Latitude"])
    dealer_lon = float(dealer["Longitude"])

    results = []
    for lead in leads:
        city = (lead.get("city") or "").strip()
        prov = (lead.get("province") or lead.get("prov") or lead.get("state") or "").strip()
        country = (lead.get("country") or country_default).strip() or country_default

        coords = geocode_city_prov(city, prov, cache, country=country)
        if not coords:
            continue

        dist = haversine_distance(dealer_lat, dealer_lon, coords[0], coords[1])
        if dist <= radius_km:
            out = dict(lead)
            out["distance_km"] = round(dist, 1)
            results.append(out)

    _save_geo_cache(cache)
    results.sort(key=lambda x: x["distance_km"])
    return results

if __name__ == "__main__":
    dealer_name = input("Dealer / Co-op location name: ").strip()
    radius = float(input("Radius km (default 100): ").strip() or "100")

    dealer = find_dealer_by_name(dealer_name)
    leads = load_leads_cache("leads_cache.json")

    nearby = leads_within_radius_of_dealer(dealer, leads, radius_km=radius)

    print(f"\nDealer: {dealer['Location']}  ({dealer['Latitude']}, {dealer['Longitude']})")
    print(f"Matches within {radius:.0f} km: {len(nearby)}\n")

    for x in nearby[:50]:
        # adjust these fields to whatever you export
        print(f"{x['distance_km']:>6} km | {x.get('name','')} | {x.get('city','')}, {x.get('province','')}")
