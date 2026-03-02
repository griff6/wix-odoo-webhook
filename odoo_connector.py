# -----------------------------
# file: odoo_connector.py
# -----------------------------
import xmlrpc.client
from datetime import datetime, date, timedelta 
import math
from typing import Optional, Union, Dict
import re
import string
import traceback
import json
import urllib.parse
import urllib.request
from pathlib import Path

ODOO_URL = 'https://wavcor-international-inc2.odoo.com'
#ODOO_URL = 'https://wavcor-test-2025-07-20.odoo.com'
ODOO_DB = 'wavcor-international-inc2'
#ODOO_DB = 'wavcor-test-2025-07-20'
#ODOO_USERNAME = 'jason@wavcor.ca'
#ODOO_PASSWORD = 'Wavcor3702?'
ODOO_USERNAME = 'sales@wavcor.ca'
ODOO_PASSWORD = 'wavcor3702'
CRM_LEAD_MODEL_ID = 1082
OSRM_BASE_URL = "https://router.project-osrm.org"
ROUTE_CACHE_PATH = Path("route_duration_cache.json")
MAX_DEALER_DRIVE_HOURS = 2.0
# Performance tuning: route only nearest candidates instead of all dealers.
MAX_ROUTE_CANDIDATES = 35
DIRECT_DISTANCE_RADIUS_PER_HOUR_KM = 120.0
DIRECT_DISTANCE_BUFFER_KM = 75.0

def _ensure_char(v):
    """Return a safe Char/Text value: empty string for None."""
    return "" if v is None else str(v)

def _ensure_id(v):
    """Return an int ID or False (Odoo null) for None/invalid."""
    if v is None or v is False or v == "":
        return False
    try:
        return int(v)
    except Exception:
        return False

def _drop_nones(d: dict) -> dict:
    """Remove keys whose value is None (keep False/empty strings)."""
    return {k: ("" if (v is None and isinstance(v, str)) else v)
            for k, v in d.items() if v is not None}

def _norm_phone(p: str) -> str:
    if not p: return ""
    return "".join(ch for ch in str(p) if ch.isdigit())

def _norm_name(s: str) -> str:
    s = ''.join(c for c in (s or "") if c in string.printable)
    return re.sub(r'\s+', ' ', s.strip()).lower()

def _name_tokens(s: str) -> set:
    return set(tok for tok in re.split(r"[^\w]+", _norm_name(s)) if tok)

def _similar_names(a: str, b: str) -> bool:
    """Cheap, fast similarity: token overlap. Adjust thresholds if needed."""
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return False
    inter = len(ta & tb)
    jacc = inter / len(ta | tb)
    # generous threshold: share at least 1 token AND Jaccard >= 0.34
    return inter >= 1 and jacc >= 0.34


def connect_odoo():
    """Establishes an XML-RPC connection to the Odoo server."""
    try:
        common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common', allow_none=True, use_datetime=True)
        version = common.version()
        #print(f"Odoo version: {version}")
        uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})
        ##print(f"Authenticated as UID: {uid}")
        if not uid:
            print("ERROR: Authentication failed. Check your Odoo credentials.")
            return None, None
        models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object', allow_none=True, use_datetime=True)
        return uid, models
    except Exception as e:
        print(f"ERROR: Failed to connect to Odoo server: {e}")
        return None, None

STATE_TO_COUNTRY_MAP = {
    # Canadian Provinces and Territories
    'AB': 'Canada', 'Alberta': 'Canada',
    'BC': 'Canada', 'British Columbia': 'Canada',
    'MB': 'Canada', 'Manitoba': 'Canada',
    'NB': 'Canada', 'New Brunswick': 'Canada',
    'NL': 'Canada', 'Newfoundland and Labrador': 'Canada',
    'NS': 'Canada', 'Nova Scotia': 'Canada',
    'ON': 'Canada', 'Ontario': 'Canada',
    'PE': 'Canada', 'Prince Edward Island': 'Canada',
    'QC': 'Canada', 'Quebec': 'Canada',
    'SK': 'Canada', 'Saskatchewan': 'Canada',
    'NT': 'Canada', 'Northwest Territories': 'Canada',
    'NU': 'Canada', 'Nunavut': 'Canada',
    'YT': 'Canada', 'Yukon': 'Canada',

    # US States
    'AL': 'United States', 'Alabama': 'United States',
    'AK': 'United States', 'Alaska': 'United States',
    'AZ': 'United States', 'Arizona': 'United States',
    'AR': 'United States', 'Arkansas': 'United States',
    'CA': 'United States', 'California': 'United States',
    'CO': 'United States', 'Colorado': 'United States',
    'CT': 'United States', 'Connecticut': 'United States',
    'DE': 'United States', 'Delaware': 'United States',
    'FL': 'United States', 'Florida': 'United States',
    'GA': 'United States', 'Georgia': 'United States',
    'HI': 'United States', 'Hawaii': 'United States',
    'ID': 'United States', 'Idaho': 'United States',
    'IL': 'United States', 'Illinois': 'United States',
    'IN': 'United States', 'Indiana': 'United States',
    'IA': 'United States', 'Iowa': 'United States',
    'KS': 'United States', 'Kansas': 'United States',
    'KY': 'United States', 'Kentucky': 'United States',
    'LA': 'United States', 'Louisiana': 'United States',
    'ME': 'United States', 'Maine': 'United States',
    'MD': 'United States', 'Maryland': 'United States',
    'MA': 'United States', 'Massachusetts': 'United States',
    'MI': 'United States', 'Michigan': 'United States',
    'MN': 'United States', 'Minnesota': 'United States',
    'MS': 'United States', 'Mississippi': 'United States',
    'MO': 'United States', 'Missouri': 'United States',
    'MT': 'United States', 'Montana': 'United States',
    'NE': 'United States', 'Nebraska': 'United States',
    'NV': 'United States', 'Nevada': 'United States',
    'NH': 'United States', 'New Hampshire': 'United States',
    'NJ': 'United States', 'New Jersey': 'United States',
    'NM': 'United States', 'New Mexico': 'United States',
    'NY': 'United States', 'New York': 'United States',
    'NC': 'United States', 'North Carolina': 'United States',
    'ND': 'United States', 'North Dakota': 'United States',
    'OH': 'United States', 'Ohio': 'United States',
    'OK': 'United States', 'Oklahoma': 'United States',
    'OR': 'United States', 'Oregon': 'United States',
    'PA': 'United States', 'Pennsylvania': 'United States',
    'RI': 'United States', 'Rhode Island': 'United States',
    'SC': 'United States', 'South Carolina': 'United States',
    'SD': 'United States', 'South Dakota': 'United States',
    'TN': 'United States', 'Tennessee': 'United States',
    'TX': 'United States', 'Texas': 'United States',
    'UT': 'United States', 'Utah': 'United States',
    'VT': 'United States', 'Vermont': 'United States',
    'VA': 'United States', 'Virginia': 'United States',
    'WA': 'United States', 'Washington': 'United States',
    'WV': 'United States', 'West Virginia': 'United States',
    'WI': 'United States', 'Wisconsin': 'United States',
    'WY': 'United States', 'Wyoming': 'United States',
    'DC': 'United States', 'District of Columbia': 'United States', # Not a state, but often included

    # Australian States and Territories
    'NSW': 'Australia', 'New South Wales': 'Australia',
    'VIC': 'Australia', 'Victoria': 'Australia',
    'QLD': 'Australia', 'Queensland': 'Australia',
    'SA': 'Australia', 'South Australia': 'Australia',
    'WA': 'Australia', 'Western Australia': 'Australia',
    'TAS': 'Australia', 'Tasmania': 'Australia',
    'ACT': 'Australia', 'Australian Capital Territory': 'Australia',
    'NT': 'Australia', 'Northern Territory': 'Australia',
}

# canonical codes we accept (2-letter US/CA, some 3-letter AUS codes)
CANONICAL_CODES = {
    # Canada
    "AB","BC","MB","NB","NL","NS","ON","PE","QC","SK","NT","NU","YT",
    # United States
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
    # Australia (3-letter codes)
    "NSW","VIC","QLD","SA","WA","TAS","ACT","NT",
}

# Map full names and common aliases -> canonical code
NAME_TO_CODE = {
    # Canada
    "ALBERTA": "AB",
    "BRITISH COLUMBIA": "BC",
    "MANITOBA": "MB",
    "NEW BRUNSWICK": "NB",
    "NEWFOUNDLAND AND LABRADOR": "NL",
    "NEWFOUNDLAND": "NL",
    "NOVA SCOTIA": "NS",
    "ONTARIO": "ON",
    "PRINCE EDWARD ISLAND": "PE",
    "PEI": "PE",
    "QUEBEC": "QC",
    "SASKATCHEWAN": "SK",
    "SASK": "SK",
    "NORTHWEST TERRITORIES": "NT",
    "NWT": "NT",
    "NUNAVUT": "NU",
    "YUKON": "YT",

    # United States (full names only)
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT",
    "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI",
    "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME",
    "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI",
    "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM",
    "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND",
    "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
    "WASHINGTON DC": "DC",
    "D C": "DC",
    "DC": "DC",

    # Australia (full names and common abbreviations)
    "NEW SOUTH WALES": "NSW", "NSW": "NSW",
    "VICTORIA": "VIC", "VIC": "VIC",
    "QUEENSLAND": "QLD", "QLD": "QLD",
    "SOUTH AUSTRALIA": "SA", "SA": "SA",
    "WESTERN AUSTRALIA": "WA",  # ambiguous with US WA
    "TASMANIA": "TAS", "TAS": "TAS",
    "AUSTRALIAN CAPITAL TERRITORY": "ACT", "ACT": "ACT",
    "NORTHERN TERRITORY": "NT",  # ambiguous with Canadian NT
}

# Map canonical code -> country or list of possible countries (when ambiguous)
CODE_TO_COUNTRIES = {
    # Canada (unique)
    "AB": "Canada", "BC": "Canada", "MB": "Canada", "NB": "Canada",
    "NL": "Canada", "NS": "Canada", "ON": "Canada", "PE": "Canada",
    "QC": "Canada", "SK": "Canada", "NU": "Canada", "YT": "Canada",
    # NT ambiguous: can be Canada (Northwest Territories) or Australia (Northern Territory)
    "NT": ["Canada", "Australia"],

    # United States (unique)
    "AL": "United States", "AK": "United States", "AZ": "United States",
    "AR": "United States", "CA": "United States", "CO": "United States",
    "CT": "United States", "DE": "United States", "FL": "United States",
    "GA": "United States", "HI": "United States", "ID": "United States",
    "IL": "United States", "IN": "United States", "IA": "United States",
    "KS": "United States", "KY": "United States", "LA": "United States",
    "ME": "United States", "MD": "United States", "MA": "United States",
    "MI": "United States", "MN": "United States", "MS": "United States",
    "MO": "United States", "MT": "United States", "NE": "United States",
    "NV": "United States", "NH": "United States", "NJ": "United States",
    "NM": "United States", "NY": "United States", "NC": "United States",
    "ND": "United States", "OH": "United States", "OK": "United States",
    "OR": "United States", "PA": "United States", "RI": "United States",
    "SC": "United States", "SD": "United States", "TN": "United States",
    "TX": "United States", "UT": "United States", "VT": "United States",
    "VA": "United States", "WA": ["United States", "Australia"],  # WA ambiguous
    "WV": "United States", "WI": "United States", "WY": "United States",
    "DC": "United States",

    # Australia (unique except NT/WA handled above)
    "NSW": "Australia", "VIC": "Australia", "QLD": "Australia",
    "SA": "Australia", "TAS": "Australia", "ACT": "Australia",
}


DEALER_LOCATIONS = [
    {"Location": 'ADVANTAGE - Redvers Co-op', "Latitude": 49.572757, "Longitude": -101.69799, "Contact": 'Carson Henrion', "Phone": '+1 306-840-7364'},
    {"Location": 'ARROWWOOD - Arrowwood Agro Centre & Tire Shop', "Latitude": 50.737998, "Longitude": -113.150747, "Contact": 'Cathy Christensen', "Phone": '(403) 534-3800'},
    {"Location": 'AVONHURST - Avonhurst Agro Centre', "Latitude": 50.637581, "Longitude": -104.131317, "Contact": 'Gord Mohr', "Phone": '(306) 771-2812'},
    {"Location": 'BEAUSEJOUR - Beausejour Agro Centre', "Latitude": 50.061931, "Longitude": -96.514082, "Contact": 'Courtney Peluk', "Phone": '(431) 218-9950'},
    {"Location": 'BORDERLAND - Broadview Agro Centre', "Latitude": 50.378923, "Longitude": -102.583995, "Contact": 'Aart Kohler', "Phone": '(306) 696-3038'},
    {"Location": 'BOUNDARY - Boissevain Home & Agro Centre', "Latitude": 49.230602, "Longitude": -100.055805, "Contact": 'George Bell', "Phone": '(204) 534-2411'},
    {"Location": 'BRUNO - Bruno Co-op', "Latitude": 52.262184, "Longitude": -105.524021, "Contact": 'Wayne Thoms', "Phone": '+1 306-369-7655'},
    {"Location": 'BULYEA - Bulyea Agro Centre', "Latitude": 50.986173, "Longitude": -104.86599, "Contact": 'Brad Foster', "Phone": '(306) 725-4931'},
    {"Location": 'CENTRAL ALBERTA - Crossfield Agro Centre', "Latitude": 51.427274, "Longitude": -114.030994, "Contact": 'Kevin Latimer', "Phone": '(403) 586-1452'},
    {"Location": 'CENTRAL ALBERTA - Eckville Farm Centre', "Latitude": 52.363265, "Longitude": -114.36273, "Contact": 'Kevin Latimer', "Phone": '(403) 586-1452'},
    {"Location": 'CENTRAL ALBERTA - Innisfail Agro Centre', "Latitude": 52.027465, "Longitude": -113.950235, "Contact": 'Kevin Latimer', "Phone": '(403) 586-1452'},
    {"Location": 'CENTRAL ALBERTA - Lacombe Agro Centre', "Latitude": 52.472752, "Longitude": -113.733215, "Contact": 'Kevin Latimer', "Phone": '(403) 586-1452'},
    {"Location": 'CENTRAL ALBERTA - Spruce View Farm Centre', "Latitude": 52.085722, "Longitude": -114.3106, "Contact": 'Kevin Latimer', "Phone": '(403) 586-1452'},
    {"Location": 'CENTRAL ALBERTA - Stettler Agro Centre', "Latitude": 52.322875, "Longitude": -112.71302, "Contact": 'Kevin Latimer', "Phone": '(403) 586-1452'},
    {"Location": 'CENTRAL PLAINS - Landis Agro Centre', "Latitude": 52.2, "Longitude": -108.45, "Contact": 'Jerome Ehry', "Phone": '306-948-6939'},
    {"Location": 'CENTRAL PLAINS - Plenty Agro Centre', "Latitude": 51.782889, "Longitude": -108.647739, "Contact": 'Scott Burton', "Phone": '306-932-7072'},
    {"Location": 'CENTRAL PLAINS - Rosetown Agro Centre', "Latitude": 51.554815, "Longitude": -107.991286, "Contact": 'Duane Hogan', "Phone": '(306) 882-2649'},
    {"Location": 'CLEARVIEW - Steinbach Agro Centre', "Latitude": 49.525441, "Longitude": -96.685428, "Contact": 'Jonathan Friesen', "Phone": '(204) 326-9921'},
    {"Location": 'CORNERSTONE - St. Paul Home & Agro Centre', "Latitude": 53.987647, "Longitude": -111.291114, "Contact": 'Joanne Paquette', "Phone": '(780) 646-0456'},
    {"Location": 'CORNERSTONE - Wainwright Home & Agro Centre', "Latitude": 52.840272, "Longitude": -110.851434, "Contact": 'Greg Adams', "Phone": '(587) 252-5544'},
    {"Location": 'DAUPHIN - Dauphin Agro Centre', "Latitude": 51.153509, "Longitude": -100.04425, "Contact": 'Jaco Neimann', "Phone": '(204) 622-6080'},
    {"Location": 'DAUPHIN - Ste. Rose du Lac Agro Centre', "Latitude": 51.059125, "Longitude": -99.519423, "Contact": 'Jaco Neimann', "Phone": '(204) 622-6080'},
    {"Location": 'DAWSON - Rolla Agro Centre', "Latitude": 55.897979, "Longitude": -120.139873, "Contact": 'John Currie', "Phone": '(250) 219-3853'},
    {"Location": 'DELTA - Luseland Home & Agro Centre', "Latitude": 52.082266, "Longitude": -109.390962, "Contact": 'Michael Kwiatkowski', "Phone": '306.228.9108'},
    {"Location": 'DELTA - Macklin Agro Centre', "Latitude": 52.32659, "Longitude": -109.93661, "Contact": 'Michael Kwiatkowski', "Phone": '306.228.9108'},
    {"Location": 'DELTA - Unity Agro Centre & Bulk Petroleum', "Latitude": 52.442851, "Longitude": -109.154899, "Contact": 'Michael Kwiatkowski', "Phone": '306.228.9108'},
    {"Location": 'DISCOVERY - North Battleford Farm Supply', "Latitude": 52.776186, "Longitude": -108.300476, "Contact": 'Richard Blais', "Phone": '(306) 445-9457'},
    {"Location": 'DOMAIN - Domain Farm Supply', "Latitude": 49.6, "Longitude": -97.316667, "Contact": 'Chris Rempel', "Phone": '(204) 736-4321'},
    {"Location": 'DUPEROW - Duperow Co-op Agro Centre', "Latitude": 52.05765, "Longitude": -107.984618, "Contact": 'Brock Thomson', "Phone": '306-948-7823'},
    {"Location": 'EVERGREEN - Evergreen Coop', "Latitude": 52.37688, "Longitude": -114.9184, "Contact": 'Wes Rea', "Phone": '403-845-2841 x 2125'},
    {"Location": 'FOAM LAKE - Foam Lake Home Centre', "Latitude": 51.63806849325906, "Longitude": -103.52045083400527, "Contact": 'Darren Chorney', "Phone": '306-272-3314'},
    {"Location": 'FOUR RIVERS - Quesnel Agro Centre & Convenience Store', "Latitude": 52.979428, "Longitude": -122.493627, "Contact": 'Gil Zekveld', "Phone": '(250) 567-4225'},
    {"Location": 'FOUR RIVERS - Vanderhoof Agro Centre', "Latitude": 54.017529, "Longitude": -124.007663, "Contact": 'Gil Zekveld', "Phone": '(250) 567-4225'},
    {"Location": 'FOUR RIVERS - Vanderhoof Home Centre', "Latitude": 54.017529, "Longitude": -124.007663, "Contact": 'Gil Zekveld', "Phone": '(250) 567-4225'},
    {"Location": 'GARDENLAND - Lowe Farm Crop Protection', "Latitude": 49.354747, "Longitude": -97.587343, "Contact": 'Jordan Goethals', "Phone": '(204) 746-2684'},
    {"Location": 'GARDENLAND - Morden Agro Centre', "Latitude": 49.192706, "Longitude": -98.101457, "Contact": 'Thomas Dixon', "Phone": '204-362-6039'},
    {"Location": 'GARDENLAND - Rosetown Agro Centre', "Latitude": 51.554815, "Longitude": -107.991286, "Contact": 'Thomas Dixon', "Phone": '204-362-6039'},
    {"Location": 'GARDENLAND - St. Joseph Agro Centre', "Latitude": 49.13375, "Longitude": -97.39143, "Contact": 'Thomas Dixon', "Phone": '204-362-6039'},
    {"Location": 'GILBERT PLAINS - Gilbert Plains Agro Centre', "Latitude": 51.149067, "Longitude": -100.488638, "Contact": 'Lyle Gouldsborough', "Phone": '204-648-4692'},
    {"Location": 'GRASSROOTS - Hazenmore Agro Centre', "Latitude": 49.68616473481982, "Longitude": -107.13758647742708, "Contact": 'Larry Wall', "Phone": '306-264-5111'},
    {"Location": 'GRASSROOTS - Limerick Agro Centre/Gas Bar', "Latitude": 49.653423, "Longitude": -106.268567, "Contact": 'Pierre Jalbert', "Phone": '(306) 263-2033'},
    {"Location": 'GRASSROOTS - Woodrow Agro Centre/Gas Bar', "Latitude": 49.694814, "Longitude": -106.725313, "Contact": 'Pierre Jalbert', "Phone": '(306) 263-2033'},
    {"Location": 'HAFFORD - Hafford Agro Centre', "Latitude": 52.729733, "Longitude": -107.356791, "Contact": 'Devon Fendelet', "Phone": '(306) 549-2166'},
    {"Location": 'HERITAGE - Brandon Agro Centre', "Latitude": 49.851114, "Longitude": -99.960892, "Contact": 'Rob Greer', "Phone": '(204) 763-8998'},
    {"Location": 'HERITAGE - Minnedosa Agro Centre', "Latitude": 50.249865, "Longitude": -99.838523, "Contact": 'Rob Greer', "Phone": '(204) 763-8998'},
    {"Location": 'HERITAGE - Strathclair Agro Centre', "Latitude": 50.404463, "Longitude": -100.395537, "Contact": 'Rob Greer', "Phone": '(204) 763-8998'},
    {"Location": 'HOMESTEAD - MacGregor Farm & Building Centre', "Latitude": 49.966285, "Longitude": -98.778816, "Contact": 'Harper Kitching', "Phone": '204-637-3030'},
    {"Location": 'HUMBOLDT - Humboldt Agro Centre', "Latitude": 52.202009, "Longitude": -105.123037, "Contact": 'Dwayne Thibault', "Phone": '(306) 682-2252'},
    {"Location": 'KINDERSLEY - Brock Cardlock', "Latitude": 51.441207, "Longitude": -108.717969, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'KINDERSLEY - Coleville Cardlock', "Latitude": 51.710641, "Longitude": -109.245206, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'KINDERSLEY - Eatonia Farm Supply', "Latitude": 51.224182, "Longitude": -109.390036, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'KINDERSLEY - Hoosier Cardlock', "Latitude": 51.625239, "Longitude": -109.73855, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'KINDERSLEY - Kerrobert Cardlock', "Latitude": 51.915887, "Longitude": -109.136544, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'KINDERSLEY - Kindersley Co-op', "Latitude": 51.224182, "Longitude": -109.390036, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'KINDERSLEY - Marengo Cardlock', "Latitude": 51.480717, "Longitude": -109.780565, "Contact": 'Brent Jones', "Phone": '306-460-5717'},
    {"Location": 'LA CRETE - La Crete Co-op', "Latitude": 58.19095, "Longitude": -116.395711, "Contact": 'James Wieler', "Phone": '(780) 926-0662'},
    {"Location": 'LAKE COUNTRY - Kinistino Agro Centre', "Latitude": 52.952775, "Longitude": -105.029297, "Contact": 'Aaron Hansen', "Phone": '(306) 864-2202'},
    {"Location": 'LAKE COUNTRY - Nipawin Co-op', "Latitude": 53.362788, "Longitude": -104.017425, "Contact": 'Jim Norrish', "Phone": '(306)862-4595'},
    {"Location": 'LAKE COUNTRY - Prince Albert Agro Centre', "Latitude": 53.201097, "Longitude": -105.748901, "Contact": 'Murray Watchel', "Phone": '(306) 922-2476'},
    {"Location": 'LAKE COUNTRY - Shellbrook Agro Centre', "Latitude": 53.22248, "Longitude": -106.387482, "Contact": 'Jeff Rothwell', "Phone": '(306) 747-2122'},
    {"Location": 'LAKE COUNTRY - Spiritwood Farm Supply', "Latitude": 53.365233, "Longitude": -107.517502, "Contact": 'Shane Colley', "Phone": '(306) 883-8782'},
    {"Location": 'LAKE LENORE - Lake Lenore Agro Centre', "Latitude": 52.396159, "Longitude": -104.983562, "Contact": 'Liam Jennett', "Phone": '(306) 917-8115'},
    {"Location": 'LAKELAND - Bonnyville Agro Centre', "Latitude": 54.267925, "Longitude": -110.741515, "Contact": 'Conrad Jonker', "Phone": '(587) 257-5277'},
    {"Location": 'LEDUC - Leduc Co-op Agro Centre', "Latitude": 53.26127, "Longitude": -113.811426, "Contact": 'Marcia Williams', "Phone": '(780) 239-7560'},
    {"Location": 'LEDUC - Leduc Home & Agro Centre', "Latitude": 53.260782, "Longitude": -113.551168, "Contact": 'Marcia Williams', "Phone": '(780) 239-7560'},
    {"Location": 'LEGACY - Churchbridge Home & Agro Centre', "Latitude": 50.900089, "Longitude": -101.893302, "Contact": 'Brandon Coppicus', "Phone": '(306)620-7410'},
    {"Location": 'LEGACY - Ebenezer Agro Centre', "Latitude": 51.370535, "Longitude": -102.447115, "Contact": 'Brandon Coppicus', "Phone": '(306)620-7410'},
    {"Location": 'LEGACY - Theodore Agro Centre', "Latitude": 51.423128089348154, "Longitude": -102.9223869601466, "Contact": 'Brandon Coppicus', "Phone": '(306)620-7410'},
    {"Location": 'LEGACY - Yorkton Agro Centre and Cardlock', "Latitude": 51.212045, "Longitude": -102.461243, "Contact": 'Brandon Coppicus', "Phone": '(306)620-7410'},
    {"Location": 'LIVING SKY - Peebles Home & Agro Centre', "Latitude": 50.15999, "Longitude": -102.945666, "Contact": 'Darrell Mytopher', "Phone": '(306) 224-4521'},
    {"Location": 'LLOYDMINSTER - Lashburn Agro Centre', "Latitude": 53.136241, "Longitude": -109.628509, "Contact": 'Will Pretorius', "Phone": '(306) 285-3888'},
    {"Location": 'LLOYDMINSTER - Lloyd South Agro Centre', "Latitude": 53.281667, "Longitude": -109.986339, "Contact": 'Lindsay Gibbons', "Phone": '(780) 870-0167'},
    {"Location": 'LLOYDMINSTER - Lloydminster Agro Centre', "Latitude": 53.28437168181006, "Longitude": -109.99271994027931, "Contact": 'Lindsay Gibbons', "Phone": '(780) 870-0167'},
    {"Location": 'LLOYDMINSTER - Neilburg Agro & Lumber Centre', "Latitude": 52.83766, "Longitude": -109.627995, "Contact": 'Lindsay Gibbons', "Phone": '(780) 870-0167'},
    {"Location": 'MEADOW LAKE - Meadow Lake Co-op', "Latitude": 54.13026, "Longitude": -108.435059, "Contact": 'Gord Kohls', "Phone": '306-236-4474'},
    {"Location": 'MOOSE JAW - Avonlea Home & Agro Centre', "Latitude": 50.014587, "Longitude": -105.055646, "Contact": 'Mike Heistad', "Phone": '306-868-7445'},
    {"Location": 'MOOSE JAW - Moose Jaw Agro Centre', "Latitude": 50.396721, "Longitude": -105.535666, "Contact": 'Wendell Reimer', "Phone": '306-690-6657'},
    {"Location": 'MOOSEHORN - Moosehorn Hardware', "Latitude": 51.28905, "Longitude": -98.422117, "Contact": 'Andy Friesen', "Phone": '(204) 768-2770'},
    {"Location": 'NEEPAWA-GLADSTONE - Gladstone Agro Centre', "Latitude": 50.22549, "Longitude": -98.951066, "Contact": 'Miles Kushner', "Phone": '(204) 476-3431'},
    {"Location": 'NEEPAWA-GLADSTONE - Neepawa Agro Centre', "Latitude": 50.228417, "Longitude": -99.466401, "Contact": 'Miles Kushner', "Phone": '(204) 476-3431'},
    {"Location": 'NEERLANDIA - Neerlandia Coop Home & Farm, Agro Centre', "Latitude": 54.172301, "Longitude": -114.548565, "Contact": 'Don Carlson', "Phone": '(780) 674-2820'},
    {"Location": 'NORQUAY - Norquay Co-op', "Latitude": 51.9833, "Longitude": -102.35, "Contact": 'Gerald Fehr', "Phone": '306-594-2215'},
    {"Location": 'NORTH CORRIDOR - Thorhild Home & Agro Centre', "Latitude": 54.250782, "Longitude": -113.057067, "Contact": 'Thomas Pelletier', "Phone": '(780) 349-0303'},
    {"Location": 'NORTH COUNTRY - Plamondon Home, Energy & Agro Centre', "Latitude": 54.848511, "Longitude": -112.343948, "Contact": 'Boyd', "Phone": '(780) 798-3827'},
    {"Location": 'PARKLAND - Hudson Bay Agro Centre', "Latitude": 52.856598, "Longitude": -102.396807, "Contact": 'Karissa Lupuliak', "Phone": '(306) 865-2288'},
    {"Location": 'PARKLAND - Porcupine Plain Agro Centre', "Latitude": 52.601646719072605, "Longitude": -103.2594190501782, "Contact": 'Don Hilash', "Phone": '306-278-3113'},
    {"Location": 'PARKWAY - Roblin Agro Centre', "Latitude": 51.228216, "Longitude": -101.355058, "Contact": 'James Herman', "Phone": '(204) 937-6402'},
    {"Location": 'PEMBINA - Baldur Agro Centre', "Latitude": 49.385578, "Longitude": -99.24384, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Cypress River Agro Centre', "Latitude": 49.47232, "Longitude": -98.862716, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Glenboro Agro Centre', "Latitude": 49.555833, "Longitude": -99.291111, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Homewood Agro Centre', "Latitude": 49.508926, "Longitude": -97.865911, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Manitou Agro Centre', "Latitude": 49.240555, "Longitude": -98.536667, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Manitou Hardware', "Latitude": 51.675148, "Longitude": -105.465539, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Mariapolis Agro Centre', "Latitude": 49.360666, "Longitude": -98.98952, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Minto Agro Centre', "Latitude": 49.4312, "Longitude": -100.2227, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Notre Dame Agro Centre', "Latitude": 49.529002, "Longitude": -98.55666, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - St. Leon Agro Centre', "Latitude": 49.362883, "Longitude": -98.590628, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PEMBINA - Swan Lake Agro Centre', "Latitude": 52.06581, "Longitude": -101.29976, "Contact": 'Scott Hainsworth', "Phone": '204-723-0249'},
    {"Location": 'PINCHER CREEK - Cowley Home & Agro Centre', "Latitude": 49.56901, "Longitude": -114.073325, "Contact": 'Mike May', "Phone": '(403) 928-5021'},
    {"Location": 'PINCHER CREEK - Pincher Creek Home & Agro Centre', "Latitude": 49.485667, "Longitude": -113.950292, "Contact": 'Mike May', "Phone": '(403) 928-5021'},
    {"Location": 'PIONEER - Abbey Farm Centre', "Latitude": 50.7369, "Longitude": -108.7575, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Cabri Agro Centre', "Latitude": 50.62, "Longitude": -108.46, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Central Butte Home & Agro Centre', "Latitude": 50.79383, "Longitude": -106.507831, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Consul Agro Centre & Food Store', "Latitude": 49.295378, "Longitude": -109.520113, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Craik Home & Agro Centre', "Latitude": 51.051506, "Longitude": -105.815931, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Eastend Agro Centre', "Latitude": 49.51, "Longitude": -108.82, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Frontier Agro Centre', "Latitude": 49.204894, "Longitude": -108.561809, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Gull Lake Agro Centre & Gas Bar', "Latitude": 50.1, "Longitude": -108.4, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Hazlet Agro Centre', "Latitude": 50.400062, "Longitude": -108.593949, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Herbert Farm Centre & Gas Bar', "Latitude": 50.426316, "Longitude": -107.220189, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Kyle Agro Centre', "Latitude": 50.832702, "Longitude": -108.039213, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Maple Creek Agro Centre', "Latitude": 49.8, "Longitude": -109.1, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Morse Farm Centre', "Latitude": 50.33425, "Longitude": -106.9663, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Ponteix Farm Centre', "Latitude": 49.74138, "Longitude": -107.469433, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Sceptre Farm Centre', "Latitude": 50.86281, "Longitude": -109.27075, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Shamrock Agro Centre and Cardlock', "Latitude": 50.195306, "Longitude": -106.637098, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Shaunavon Agronomy', "Latitude": 49.644498, "Longitude": -108.415893, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Shaunavon Home & Agro Centre', "Latitude": 49.644498, "Longitude": -108.415893, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Stewart Valley Farm Centre', "Latitude": 50.596911, "Longitude": -107.806822, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Swift Current Agro', "Latitude": 50.285765, "Longitude": -107.851187, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Tompkins Farm Centre', "Latitude": 50.067518, "Longitude": -108.805208, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PIONEER - Tugaske Home & Agro Centre', "Latitude": 50.874074, "Longitude": -106.286491, "Contact": 'Zane Banadyga', "Phone": '306.750.6603'},
    {"Location": 'PRAIRIE - Cupar Agro Centre', "Latitude": 50.947688, "Longitude": -104.213978, "Contact": 'Cory Hart', "Phone": '306-726-7755'},
    {"Location": 'PRAIRIE - Ituna Home & Agro', "Latitude": 51.170571, "Longitude": -103.495756, "Contact": 'Cory Hart', "Phone": '306-726-7755'},
    {"Location": 'PRAIRIE - Kelliher Agro Centre and Gas Bar', "Latitude": 51.266667, "Longitude": -103.733333, "Contact": 'Cory Hart', "Phone": '306-726-7755'},
    {"Location": 'PRAIRIE - Lipton Farm Supply', "Latitude": 50.901424, "Longitude": -103.850734, "Contact": 'Cory Hart', "Phone": '306-726-7755'},
    {"Location": 'PRAIRIE - Mclean Agro Centre', "Latitude": 50.51894062820962, "Longitude": -104.00008109272711, "Contact": 'Cory Hart', "Phone": '306-726-7755'},
    {"Location": 'PRAIRIE - Melville Home & Agro Centre', "Latitude": 50.930724, "Longitude": -102.807296, "Contact": 'Cory Hart', "Phone": '306-726-7755'},
    {"Location": 'PRAIRIE NORTH - Archerwill Agro Centre', "Latitude": 52.439137, "Longitude": -103.863044, "Contact": 'Shanese Martin', "Phone": '306-921-6677'},
    {"Location": 'PRAIRIE NORTH - Kelvington Home & Agro Centre', "Latitude": 52.164397, "Longitude": -103.522808, "Contact": 'Natalie Pitt', "Phone": '306-620-3973'},
    {"Location": 'PRAIRIE NORTH - Melfort Home & Agro Centre', "Latitude": 52.861371, "Longitude": -104.613554, "Contact": 'Allen Woolsey', "Phone": '306-921-9800'},
    {"Location": 'PRAIRIE NORTH - Naicam Agro Centre', "Latitude": 52.416507, "Longitude": -104.496329, "Contact": 'Shane Klepak', "Phone": '306-921-9598'},
    {"Location": 'PRAIRIE ROOTS - Elm Creek Agro Centre', "Latitude": 49.8, "Longitude": -109.1, "Contact": 'Bob Thiessen', "Phone": '204-781-2447'},
    {"Location": 'PRAIRIE ROOTS - Marquette Agro Centre', "Latitude": 50.064485, "Longitude": -97.734061, "Contact": 'Bob Thiessen', "Phone": '204-781-2447'},
    {"Location": 'PRAIRIE ROOTS - Starbuck Agro Centre', "Latitude": 49.768476, "Longitude": -97.618268, "Contact": 'Bob Thiessen', "Phone": '204-781-2447'},
    {"Location": 'PRAIRIE SKY - Lang Agro Centre', "Latitude": 49.918915, "Longitude": -104.371512, "Contact": 'Michael Saip', "Phone": '(306) 464-2008'},
    {"Location": 'RAMA - Rama Agro Main Store', "Latitude": 51.760151, "Longitude": -102.997915, "Contact": 'Glenda Jeffery', "Phone": '306-814-8701'},
    {"Location": 'RIVERBEND - Beechy Farm Supply', "Latitude": 50.879328, "Longitude": -107.383858, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Broderick Agro Centre', "Latitude": 51.592881, "Longitude": -106.9175, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Davidson Home Agro & Liquor', "Latitude": 51.262855, "Longitude": -105.989034, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Lucky Lake Agro Centre', "Latitude": 50.985022, "Longitude": -107.137321, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Outlook Home & Agro', "Latitude": 51.48866, "Longitude": -107.05039, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Riverbend Coop at Lanigan', "Latitude": 51.83383172272621, "Longitude": -104.98000611328575, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Riverbend Coop at Nakomis', "Latitude": 51.50569635926104, "Longitude": -105.0018226034098, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Strongfield Agro Centre', "Latitude": 51.33159, "Longitude": -106.58952, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Tullis Agro Centre', "Latitude": 51.038626, "Longitude": -107.037085, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Wiseton Farm Supply', "Latitude": 51.315073, "Longitude": -107.650071, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERBEND - Young Gas Bar and Agro', "Latitude": 51.769073, "Longitude": -105.748323, "Contact": 'Tony Britnell', "Phone": '306.867.7672'},
    {"Location": 'RIVERSIDE - Carrot River Farm Supply', "Latitude": 53.284101, "Longitude": -103.584595, "Contact": 'Tammy Doerksen', "Phone": '(306) 873-5111'},
    {"Location": 'RIVERSIDE - Tisdale Agro Centre', "Latitude": 52.850059, "Longitude": -104.048767, "Contact": 'Tammy Doerksen', "Phone": '(306) 873-5111'},
    {"Location": 'SASKATOON - Co-op Agro Centre (Colonsay)', "Latitude": 51.980557, "Longitude": -105.86921, "Contact": 'Volodymyr Vakula', "Phone": '306-917-8778'},
    {"Location": 'SASKATOON - Co-op Farm & Hardware (Watrous)', "Latitude": 51.675148, "Longitude": -105.465539, "Contact": 'Volodymyr Vakula', "Phone": '306-917-8778'},
    {"Location": 'SASKATOON - Hepburn Agro Centre', "Latitude": 52.524511, "Longitude": -106.731207, "Contact": 'Volodymyr Vakula', "Phone": '306-917-8778'},
    {"Location": 'SASKATOON - Saskatoon Co-op Agro', "Latitude": 52.1332, "Longitude": -106.67, "Contact": 'Volodymyr Vakula', "Phone": '306-917-8778'},
    {"Location": 'SEDALIA - Sedalia Agro Centre and Food Store', "Latitude": 51.675493, "Longitude": -110.665261, "Contact": 'Ed Thornton', "Phone": '(403) 326-2152'},
    {"Location": 'SHERWOOD - Montmartre Agro Centre', "Latitude": 50.21761, "Longitude": -103.448371, "Contact": 'Todd Douan', "Phone": '(306) 424-2293'},
    {"Location": 'SOUTH COUNTRY - Barons Fertilizer/Agro', "Latitude": 49.997493, "Longitude": -113.081528, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Brooks Agro Centre', "Latitude": 50.571027, "Longitude": -111.893005, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Broxburn Co-op', "Latitude": 49.694578, "Longitude": -112.833103, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Claresholm Fertilizer/Agro', "Latitude": 50.023946, "Longitude": -113.580028, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Foremost Agro Centre', "Latitude": 49.477047, "Longitude": -111.445316, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - High River Fertilizer/Agro', "Latitude": 50.580192, "Longitude": -113.870933, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Lethbridge County Agro and Crop Protection', "Latitude": 53.284101, "Longitude": -103.584595, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Lomond Agro Centre & Fertilizer', "Latitude": 50.350439, "Longitude": -112.641492, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Medicine Hat Agro & Hardware Centre', "Latitude": 50.201734, "Longitude": -110.515021, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Oyen Bulk Petroleum and Agro', "Latitude": 51.34889, "Longitude": -110.487197, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTH COUNTRY - Vauxhall Home & Agro Centre & Tire Shop', "Latitude": 50.069185, "Longitude": -112.107691, "Contact": 'Jerry Gooch', "Phone": '(403) 715-2644'},
    {"Location": 'SOUTHERN PLAINS - Estevan Bulk & Agro', "Latitude": 49.142808, "Longitude": -102.991075, "Contact": 'Jason Macdonald', "Phone": '(306) 637-4330'},
    {"Location": 'SOUTHLAND - Assiniboia Agro Centre', "Latitude": 49.631119, "Longitude": -105.992649, "Contact": 'Casey Topola', "Phone": '(306) 642-4933'},
    {"Location": 'SOUTHLAND - Coronach Service Centre', "Latitude": 49.11197361209381, "Longitude": -105.50724720141736, "Contact": 'Casey Topola', "Phone": ''},
    {"Location": 'SOUTHLAND - Mossbank Service Centre', "Latitude": 49.936604581241845, "Longitude": -105.96481303771006, "Contact": 'Casey Topola', "Phone": ''},
    {"Location": 'ST. ISIDORE - Falher Agro Centre', "Latitude": 55.734688, "Longitude": -117.201276, "Contact": 'Jeff Labreque', "Phone": '780-625-9043'},
    {"Location": 'ST. ISIDORE - St. Isidore Agro Centre & Food Store', "Latitude": 56.205507, "Longitude": -117.104895, "Contact": 'Scott Shearer', "Phone": '(780) 618-7834'},
    {"Location": 'SWAN VALLEY - Swan River Agro Centre', "Latitude": 52.06581, "Longitude": -101.29976, "Contact": 'Tony Blazenko', "Phone": '(204) 734-4208'},
    {"Location": 'SWAN VALLEY - The Pas Agro Centre', "Latitude": 53.822417, "Longitude": -101.240515, "Contact": 'Joey Smith', "Phone": '(204) 623-6934'},
    {"Location": 'TURTLEFORD - Turtleford & District Co-op @ Maidstone', "Latitude": 53.034, "Longitude": -108.973, "Contact": 'Brent Edwards', "Phone": '306-893-1222'},
    {"Location": 'TURTLEFORD - Turtleford Farm Supply & Home Centre', "Latitude": 53.387847, "Longitude": -108.958192, "Contact": 'Kelly Svoboda', "Phone": '(306) 845-2162'},
    {"Location": 'TWIN VALLEY - Birtle Home & Agro Centre', "Latitude": 50.421841, "Longitude": -101.046201, "Contact": 'Josh Gerelus', "Phone": '(431) 257-0046'},
    {"Location": 'TWIN VALLEY - Elkhorn Agro Centre', "Latitude": 49.975341, "Longitude": -101.236587, "Contact": 'Gary Goodrich', "Phone": '204-821-8040'},
    {"Location": 'TWIN VALLEY - Foxwarren Fertilizer Plant', "Latitude": 50.421841, "Longitude": -101.046201, "Contact": 'Josh Gerelus', "Phone": '(431) 257-0046'},
    {"Location": 'TWIN VALLEY - Miniota Agro Centre', "Latitude": 50.133333, "Longitude": -101.033333, "Contact": 'Josh Gerelus', "Phone": '(431) 257-0046'},
    {"Location": 'TWIN VALLEY - Rossburn Agro Centre', "Latitude": 50.668359, "Longitude": -100.810698, "Contact": 'Josh Gerelus', "Phone": '(431) 257-0046'},
    {"Location": 'TWIN VALLEY - Russell Agro Centre', "Latitude": 50.780454, "Longitude": -101.287879, "Contact": 'Josh Gerelus', "Phone": '(431) 257-0046'},
    {"Location": 'VALLEYVIEW - Pierson Agro Centre', "Latitude": 49.17821, "Longitude": -101.262358, "Contact": 'Gary Goodrich', "Phone": '204-821-8040'},
    {"Location": 'VALLEYVIEW - Virden Agro Centre', "Latitude": 49.848509, "Longitude": -100.932265, "Contact": 'Gary Goodrich', "Phone": '204-821-8040'},
    {"Location": 'WESTVIEW - Consort Agro Centre', "Latitude": 52.00825, "Longitude": -110.76512, "Contact": 'Brian Sortland', "Phone": '403-577-2802'},
    {"Location": 'WESTVIEW - Drumheller Home & Agro Centre', "Latitude": 51.416933, "Longitude": -112.640238, "Contact": 'Todd Bossert', "Phone": '(403) 854-8420'},
    {"Location": 'WESTVIEW - Eagle Hill Agro Centre', "Latitude": 51.88229061525029, "Longitude": -114.42681335059666, "Contact": 'Brunel Dupuis', "Phone": '403-556-2113'},
    {"Location": 'WESTVIEW - Hanna Agro Centre', "Latitude": 51.644557, "Longitude": -111.927479, "Contact": 'Todd Bossert', "Phone": '(403) 854-8420'},
    {"Location": 'WESTVIEW - Horseshoe Canyon Agro Centre', "Latitude": 51.416933, "Longitude": -112.640238, "Contact": 'Allen Capnerhurst', "Phone": '(403) 994-4636'},
    {"Location": 'WESTVIEW - Olds Co-op', "Latitude": 51.80032, "Longitude": -114.099052, "Contact": 'Brunel Dupuis', "Phone": '403-556-2113'},
    {"Location": 'WETASKIWIN - Falun Gas Bar, Home & Agro Centre', "Latitude": 52.95927342921948, "Longitude": -113.83610183123061, "Contact": 'Chris Humbke', "Phone": '(780) 387-0490'},
    {"Location": 'WETASKIWIN - Wetaskiwin Agro Centre', "Latitude": 52.968492, "Longitude": -113.36792, "Contact": 'Chris Humbke', "Phone": '(780) 387-0490'},
    {"Location": 'WILD ROSE - Camrose Agro Centre', "Latitude": 52.910438, "Longitude": -112.726688, "Contact": 'Brett Njaa', "Phone": '780-385-0930'},
    {"Location": 'WILD ROSE - Head Office', "Latitude": 51.006727, "Longitude": -114.062842, "Contact": 'Neil Bratrud', "Phone": '(780) 385-8475'},
    {"Location": 'WILD ROSE - Sedgewick Home and Farm Centre', "Latitude": 52.776455, "Longitude": -111.695401, "Contact": 'Adam Creasy', "Phone": '(780) 385-5877'},
    {"Location": 'WILD ROSE - Tofield Agro Centre', "Latitude": 53.369913, "Longitude": -112.668654, "Contact": 'Brett Njaa', "Phone": '780-385-0930'},
    {"Location": 'WILD ROSE - Viking Agro Centre', "Latitude": 53.092599, "Longitude": -111.777788, "Contact": 'Adam Creasy', "Phone": '(780) 385-5877'},
    {"Location": 'WYNYARD - Wynyard Agro Centre', "Latitude": 51.7833, "Longitude": -104.1667, "Contact": 'Victor Hawryluk', "Phone": '306-874-7816'},
]


# helper to clean input strings
def _clean(value: str) -> str:
    if not value:
        return ""
    v = str(value).strip().upper()
    v = re.sub(r"[^\w\s]", "", v)  # remove punctuation
    v = re.sub(r"\s+", " ", v)     # collapse whitespace
    return v.strip()

def normalize_state(value: str) -> str:
    """
    Normalize any state/province input into a canonical code (e.g. 'ND', 'SK', 'VIC').
    Returns the canonical code if recognized, otherwise cleaned input.
    """
    if not value:
        return value
    v = _clean(value)

    # already a canonical code?
    if v in CANONICAL_CODES:
        return v

    # direct alias lookup
    if v in NAME_TO_CODE:
        return NAME_TO_CODE[v]

    # try no-space version (handles "PrinceEdwardIsland")
    v_nospace = v.replace(" ", "")
    if v_nospace in NAME_TO_CODE:
        return NAME_TO_CODE[v_nospace]

    return v  # fallback: cleaned input

def resolve_country(state_input: str, country_hint: Optional[str] = None) -> Union[str, list, None]:
    """
    Return a single country (string) when unambiguous, a list when ambiguous,
    or None when unknown. You may pass country_hint (like 'Canada' or 'Australia')
    to resolve ambiguous codes.
    """
    if not state_input:
        return None

    raw = str(state_input)
    code = normalize_state(raw)
    countries = CODE_TO_COUNTRIES.get(code)

    if not countries:
        return None

    # single country string -> done
    if isinstance(countries, str):
        return countries

    # countries is a list (ambiguous): try country_hint first
    if country_hint:
        h = country_hint.strip().lower()
        for c in countries:
            if h in c.lower():
                return c

    # try to disambiguate by words in the original input
    u = _clean(raw)
    if "NORTHWEST" in u or "NWT" in u:
        return "Canada"
    if "NORTHERN TERRIT" in u:  # "NORTHERN TERRITORY"
        return "Australia"
    if "WESTERN AUSTRALIA" in u or ("AUSTRALIA" in u and "WESTERN" in u):
        return "Australia"
    if "WASHINGTON" in u and "DC" not in u:
        return "United States"

    # cannot disambiguate
    return countries

def get_state_id(models, uid, state_name):
    if not state_name:
        return False
    
    normalized_state = normalize_state(state_name)
    if not normalized_state:
        #print(f"DEBUG: No normalized state found for '{state_name}'.")
        return False
        
    #print(f"DEBUG: Normalized state is '{normalized_state}'.")
    
    country_name = resolve_country(state_name)
    if not country_name:
        #print(f"DEBUG: Could not resolve country for state '{state_name}'.")
        return False
    
    country_id = get_country_id(models, uid, country_name)
    if not country_id:
        print(f"DEBUG: Could not get country ID for '{country_name}'.")
        return False

    # 1. Search for an exact match on the code first
    states = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
        'res.country.state', 'search_read',
        [[('code', '=', normalized_state), ('country_id', '=', country_id)]], 
        {'fields': ['id'], 'limit': 1})
        
    if states:
        #print(f"DEBUG: Found Odoo state ID {states[0]['id']} by code for '{state_name}'.")
        return states[0]['id']
    else:
        # 2. Fallback to a case-insensitive name search if no code match is found
        states = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            'res.country.state', 'search_read',
            [[('name', 'ilike', state_name.strip()), ('country_id', '=', country_id)]], 
            {'fields': ['id'], 'limit': 1})
            
        if states:
            #print(f"DEBUG: Found Odoo state ID {states[0]['id']} by name for '{state_name}'.")
            return states[0]['id']
        else:
            #print(f"DEBUG: No Odoo state found for '{state_name}' in country '{country_name}'.")
            return False

def get_country_id(models, uid, country_name):
    if not country_name:
        return False
    try:
        country = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            'res.country', 'search_read',
            [[('name', '=', country_name.strip())]], {'fields': ['id'], 'limit': 1})
        return country[0]['id'] if country else False
    except xmlrpc.client.Fault as e:
        #print(f"Odoo RPC Error getting country ID for '{country_name}': {e.faultString}")
        return False
    except Exception as e:
        #print(f"Unexpected error getting country ID for '{country_name}': {e}")
        return False
    
def get_state_and_country_ids(models, uid: int, prov_state_raw: str):
    """
    Uses your existing normalize_state + resolve_country + get_state_id.
    Returns (state_id, country_id). Either may be False.
    """
    st_code = normalize_state(prov_state_raw or "")
    if not st_code:
        return False, False

    country_code = resolve_country(st_code)  # <- your existing logic (CA/US/AU)
    country_id = get_country_id(models, uid, country_code) if country_code else False

    # This must already search res.country.state with (code, country_id)
    state_id = get_state_id(models, uid, prov_state_raw)  # <- your existing function

    # If resolve_country succeeded but state_id didn’t (rare), still return country_id
    return state_id or False, country_id or False

    
def get_model_id(models, uid, model_name):
    """
    Gets the Odoo ID for a given model (e.g., 'crm.lead').
    """
    try:
        model_record = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            'ir.model', 'search_read',
            [[('model', '=', model_name)]], {'fields': ['id'], 'limit': 1})
        if model_record:
            return model_record[0]['id']
        else:
            print(f"ERROR: Model '{model_name}' not found in Odoo.")
            return False
    except xmlrpc.client.Fault as e:
        print(f"Odoo RPC Error getting model ID for '{model_name}': {e.faultString}")
        return False
    except Exception as e:
        print(f"Unexpected error getting model ID for '{model_name}': {e}")
        return False

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points on Earth using the Haversine formula.
    Latitudes and Longitudes are in decimal degrees.
    Returns distance in kilometers.
    """
    R = 6371  # Radius of Earth in kilometers

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def _route_key(lat1: float, lon1: float, lat2: float, lon2: float) -> str:
    return f"{lat1:.6f},{lon1:.6f}->{lat2:.6f},{lon2:.6f}"


def _load_route_cache() -> dict:
    if not ROUTE_CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(ROUTE_CACHE_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_route_cache(cache: dict) -> None:
    try:
        ROUTE_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"WARNING: Failed to save route cache: {e}")


def _osrm_route_metrics(lat1: float, lon1: float, lat2: float, lon2: float, cache: dict) -> Optional[dict]:
    key = _route_key(lat1, lon1, lat2, lon2)
    if key in cache:
        entry = cache[key] or {}
        if entry.get("duration_s") is not None and entry.get("distance_m") is not None:
            return entry

    coords = f"{lon1},{lat1};{lon2},{lat2}"
    url = f"{OSRM_BASE_URL}/route/v1/driving/{coords}"
    params = urllib.parse.urlencode({"overview": "false", "alternatives": "false"})
    try:
        with urllib.request.urlopen(f"{url}?{params}", timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        routes = payload.get("routes") or []
        if not routes:
            return None
        route = routes[0]
        duration_s = float(route.get("duration"))
        distance_m = float(route.get("distance"))
        cache[key] = {"duration_s": duration_s, "distance_m": distance_m}
        return cache[key]
    except Exception:
        return None


def _osrm_table_metrics_one_to_many(
    src_lat: float,
    src_lon: float,
    dests: list,
) -> Dict[int, dict]:
    """
    Fetch drive duration/distance from one source to many destinations using OSRM table API.
    dests: list of tuples (idx, dest_lat, dest_lon)
    Returns: {idx: {"duration_s": float, "distance_m": float}}
    """
    if not dests:
        return {}

    # coords: source first, then destinations
    coord_parts = [f"{src_lon},{src_lat}"] + [f"{lon},{lat}" for _, lat, lon in dests]
    coords = ";".join(coord_parts)
    destinations = ";".join(str(i) for i in range(1, len(coord_parts)))
    params = urllib.parse.urlencode(
        {
            "sources": "0",
            "destinations": destinations,
            "annotations": "duration,distance",
        }
    )
    url = f"{OSRM_BASE_URL}/table/v1/driving/{coords}?{params}"
    with urllib.request.urlopen(url, timeout=25) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    durations = (payload.get("durations") or [[]])[0]
    distances = (payload.get("distances") or [[]])[0]
    out = {}
    for i, (idx, _, _) in enumerate(dests):
        dur = durations[i] if i < len(durations) else None
        dist = distances[i] if i < len(distances) else None
        if dur is None or dist is None:
            continue
        out[idx] = {"duration_s": float(dur), "distance_m": float(dist)}
    return out


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).casefold()


def _compact_alnum(s: str) -> str:
    return "".join(ch for ch in _norm_key(s) if ch.isalnum())


def _match_dealer_option_value_by_location(selection_options: list, dealer_location: str) -> Optional[str]:
    """
    Map a dealer location string (e.g. 'Redvers Co-op') to a Dealer property option key.
    Selection options come in shape: [[key, label], ...].
    """
    if not dealer_location:
        return None
    needle = _norm_key(dealer_location)
    needle_compact = _compact_alnum(dealer_location)
    if not needle:
        return None

    labels = []
    for option in selection_options or []:
        if isinstance(option, (list, tuple)) and len(option) >= 2:
            labels.append((str(option[0]), str(option[1])))

    exact = [opt for opt in labels if _norm_key(opt[1]) == needle]
    if len(exact) == 1:
        return exact[0][0]

    contains = [opt for opt in labels if needle in _norm_key(opt[1]) or (needle_compact and needle_compact in _compact_alnum(opt[1]))]
    if len(contains) == 1:
        return contains[0][0]
    if len(contains) > 1:
        contains_sorted = sorted(contains, key=lambda x: len(_norm_key(x[1])))
        return contains_sorted[-1][0]

    reverse = [opt for opt in labels if _norm_key(opt[1]) in needle or (_compact_alnum(opt[1]) and _compact_alnum(opt[1]) in needle_compact)]
    if len(reverse) == 1:
        return reverse[0][0]
    return None


def _jsonrpc_call(service: str, method: str, *args):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"service": service, "method": method, "args": list(args)},
        "id": 1,
    }
    req = urllib.request.Request(
        f"{ODOO_URL.rstrip('/')}/jsonrpc",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data["result"]


def _jsonrpc_execute_kw(model: str, method: str, args: list, kwargs: Optional[dict] = None):
    if kwargs is None:
        kwargs = {}
    uid = _jsonrpc_call("common", "login", ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    if not uid:
        raise RuntimeError("JSON-RPC authentication failed.")
    return _jsonrpc_call(
        "object",
        "execute_kw",
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        model,
        method,
        args,
        kwargs,
    )


def find_closest_dealer(customer_lat, customer_lon, max_drive_hours: float = MAX_DEALER_DRIVE_HOURS):
    """
    Find the closest dealer by DRIVING distance, but only among dealers within max_drive_hours.
    Evaluates all dealers (not first match), then picks the shortest driving distance.
    Returns dealer dict with Distance_km and Drive_time_hr, or None if no route/dealer in threshold.
    """
    if not DEALER_LOCATIONS:
        print("No dealer locations defined.")
        return None

    max_duration_s = max_drive_hours * 3600.0
    cache = _load_route_cache()
    cache_dirty = False
    candidates = []
    print(
        f"DEBUG dealer: start lookup lat={customer_lat}, lon={customer_lon}, "
        f"max_hours={max_drive_hours}",
        flush=True,
    )

    direct_rows = []
    for idx, dealer in enumerate(DEALER_LOCATIONS):
        dealer_lat = dealer.get("Latitude")
        dealer_lon = dealer.get("Longitude")
        if dealer_lat is None or dealer_lon is None:
            continue
        direct_km = haversine_distance(
            float(customer_lat), float(customer_lon), float(dealer_lat), float(dealer_lon)
        )
        direct_rows.append((direct_km, idx, dealer, float(dealer_lat), float(dealer_lon)))

    if not direct_rows:
        print("No dealers with coordinates available.", flush=True)
        return None

    # Pre-filter candidates by direct distance so first-lookups are fast.
    # We still use routing for final winner among this subset.
    direct_rows.sort(key=lambda row: row[0])
    radius_km = (max_drive_hours * DIRECT_DISTANCE_RADIUS_PER_HOUR_KM) + DIRECT_DISTANCE_BUFFER_KM
    subset = [row for row in direct_rows if row[0] <= radius_km]
    if len(subset) < MAX_ROUTE_CANDIDATES:
        subset = direct_rows[:MAX_ROUTE_CANDIDATES]
    elif len(subset) > MAX_ROUTE_CANDIDATES:
        subset = subset[:MAX_ROUTE_CANDIDATES]

    missing = []
    all_rows = []
    for _direct_km, idx, dealer, dealer_lat, dealer_lon in subset:
        key = _route_key(float(customer_lat), float(customer_lon), float(dealer_lat), float(dealer_lon))
        entry = cache.get(key)
        if entry and entry.get("duration_s") is not None and entry.get("distance_m") is not None:
            all_rows.append((idx, dealer, key, float(entry["duration_s"]), float(entry["distance_m"])))
        else:
            missing.append((idx, dealer, key, dealer_lat, dealer_lon))
    print(
        f"DEBUG dealer: candidates={len(subset)} routes_cached={len(all_rows)} uncached={len(missing)}",
        flush=True,
    )

    # Batch request uncached routes to avoid N requests per lead.
    BATCH = 40
    for i in range(0, len(missing), BATCH):
        chunk = missing[i:i + BATCH]
        dests = [(idx, lat, lon) for idx, _, _, lat, lon in chunk]
        try:
            metrics_by_idx = _osrm_table_metrics_one_to_many(float(customer_lat), float(customer_lon), dests)
        except Exception:
            print(
                f"WARNING dealer: OSRM table batch failed for {len(chunk)} destinations.",
                flush=True,
            )
            metrics_by_idx = {}

            # Fallback: table endpoint can fail/rate-limit; retry with single route calls.
            for idx, dealer, key, dlat, dlon in chunk:
                m = _osrm_route_metrics(
                    float(customer_lat),
                    float(customer_lon),
                    float(dlat),
                    float(dlon),
                    cache,
                )
                if m and m.get("duration_s") is not None and m.get("distance_m") is not None:
                    metrics_by_idx[idx] = {
                        "duration_s": float(m["duration_s"]),
                        "distance_m": float(m["distance_m"]),
                    }
        for idx, dealer, key, _lat, _lon in chunk:
            m = metrics_by_idx.get(idx)
            if not m:
                continue
            cache[key] = {"duration_s": m["duration_s"], "distance_m": m["distance_m"]}
            cache_dirty = True
            all_rows.append((idx, dealer, key, float(m["duration_s"]), float(m["distance_m"])))
    print(
        f"DEBUG dealer: routes available after fetch={len(all_rows)}",
        flush=True,
    )

    for _idx, dealer, _key, duration_s, distance_m in all_rows:
        if duration_s <= max_duration_s:
            candidates.append((distance_m, duration_s, dealer))

    if cache_dirty:
        _save_route_cache(cache)

    if not candidates:
        print(
            f"INFO: No dealer found within {max_drive_hours:.1f}h driving "
            f"for ({customer_lat}, {customer_lon})."
        )
        return None

    candidates.sort(key=lambda row: (row[0], row[1]))
    distance_m, duration_s, dealer = candidates[0]
    result = dict(dealer)
    result["Distance_km"] = round(distance_m / 1000.0, 2)
    result["Drive_time_hr"] = round(duration_s / 3600.0, 2)
    print(
        f"DEBUG dealer: in_range={len(candidates)} selected='{result.get('Location')}' "
        f"distance_km={result['Distance_km']} drive_hr={result['Drive_time_hr']}",
        flush=True,
    )
    return result


def set_dealer_property_on_lead(models, uid, lead_id: int, dealer_location: str, property_label: str = "Dealer") -> bool:
    """
    Set only the Dealer property value on crm.lead.lead_properties.
    Does not modify any other lead fields.
    """
    try:
        # Use JSON-RPC for properties payloads; XML-RPC can fail if None is nested in lead_properties.
        print(
            f"DEBUG dealer_property: lead_id={lead_id} target_dealer='{dealer_location}'",
            flush=True,
        )
        rows = _jsonrpc_execute_kw(
            "crm.lead",
            "read",
            [[int(lead_id)]],
            {"fields": ["lead_properties"]},
        )
        if not rows:
            return False
        props = rows[0].get("lead_properties") or []

        changed = False
        for item in props:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "selection":
                continue
            if _norm_key(item.get("string") or "") != _norm_key(property_label):
                continue
            opt_value = _match_dealer_option_value_by_location(item.get("selection") or [], dealer_location)
            if not opt_value:
                print(f"WARNING: Could not map dealer '{dealer_location}' to a Dealer property option.")
                return False
            print(
                f"DEBUG dealer_property: mapped dealer='{dealer_location}' -> option_value='{opt_value}'",
                flush=True,
            )
            if str(item.get("value") or "") == str(opt_value):
                print("DEBUG dealer_property: already set; no write required.", flush=True)
                return True
            item["value"] = str(opt_value)
            changed = True
            break

        if not changed:
            print(f"WARNING: Dealer property '{property_label}' not found on lead {lead_id}.")
            return False

        ok = _jsonrpc_execute_kw(
            "crm.lead",
            "write",
            [[int(lead_id)], {"lead_properties": props}],
        )
        print(f"DEBUG dealer_property: write result={ok}", flush=True)
        return bool(ok)
    except Exception as e:
        print(f"ERROR setting dealer property for lead {lead_id}: {e}")
        return False


def get_or_create_tags(models, uid, tags):
    tag_ids = []
    for tag in tags:
        tag = tag.strip()
        if not tag:
            continue

        if tag == "Airblast Fans":
            tag = "Airblast"
        if tag == "DryIT Radial Flow":
            tag = "DryIT"
            #print(f"DEBUG (connector): Transforming 'DryIT Radial Flow' to 'DryIT' for contact tag.")

        try:
            existing = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                'res.partner.category', 'search_read',
                [[('name', '=', tag)]], {'fields': ['id'], 'limit': 1})
            if existing:
                tag_ids.append(existing[0]['id'])
            else:
                new_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                    'res.partner.category', 'create', [{'name': tag}])
                tag_ids.append(new_id)
        except xmlrpc.client.Fault as e:
            print(f"Odoo RPC Error creating contact tag '{tag}': {e.faultString}")
        except Exception as e:
            print(f"Unexpected error creating contact tag '{tag}': {e}")
    return tag_ids


def get_or_create_opportunity_tags(models, uid, tags):
    """
    Gets or creates Odoo CRM Lead Tags (crm.tag) and returns their IDs.
    """
    tag_ids = []
    #print(f"DEBUG (connector): get_or_create_opportunity_tags called with tags: {tags}")
    for tag in tags:
        tag = tag.strip()
        if not tag:
            print(f"DEBUG (connector): Skipping empty tag.")
            continue

        if tag == "Airblast Fans":
            tag = "Airblast"
            ##print(f"DEBUG (connector): Transforming 'Airblast Fans' to 'Airblast' for opportunity tag.")
        if tag == "DryIT Radial Flow":
            tag = "DryIT"
            print(f"DEBUG (connector): Transforming 'DryIT Radial Flow' to 'DryIT' for opportunity tag.")

        try:
            #print(f"DEBUG (connector): Searching for opportunity tag: '{tag}'")
            existing = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                'crm.tag', 'search_read',
                [[('name', '=', tag)]], {'fields': ['id'], 'limit': 1})
            if existing:
                tag_ids.append(existing[0]['id'])
                #print(f"DEBUG (connector): Found existing tag '{tag}' with ID: {existing[0]['id']}")
            else:
                new_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                    'crm.tag', 'create', [{'name': tag}])
                tag_ids.append(new_id)
                #print(f"DEBUG (connector): Created new tag '{tag}' with ID: {new_id}")
        except xmlrpc.client.Fault as e:
            print(f"Odoo RPC Error getting/creating opportunity tag '{tag}': {e.faultString}")
        except Exception as e:
            print(f"Unexpected error getting/creating opportunity tag '{tag}': {e}")
    #print(f"DEBUG (connector): Final tag_ids collected: {tag_ids}")
    return tag_ids


def create_odoo_contact(data):
    try:
        uid, models = connect_odoo()
        if not uid:
            print("ERROR: Could not connect to Odoo.")
            return False

        raw_name = (data.get("Name") or "").strip()
        if not raw_name:
            first = (data.get("First name") or "").strip()
            last  = (data.get("Last name")  or "").strip()
            raw_name = (f"{first} {last}").strip()
        name_val = raw_name  # <-- ensure defined before use
        if not name_val:
            raise ValueError("Contact must have a Name (or First/Last name).")
        
        prov_state_raw = data.get("Prov/State") or data.get("State") or ""
        state_id, country_id = get_state_and_country_ids(models, uid, prov_state_raw)


        # --- Build the contact payload ---
        contact_vals_raw = {
            "name": name_val,
            "email": _ensure_char(data.get("Email")),
            "phone": _ensure_char(data.get("Phone")),
            "state_id": _ensure_id(state_id) or False,
            "country_id": _ensure_id(country_id) or False,
        }

        city_val = (data.get("City") or "").strip()
        if city_val:
            contact_vals_raw["city"] = _ensure_char(city_val)

        # Remove any accidental None left in dict
        contact_vals = _drop_nones(contact_vals_raw)

        # DEBUG visibility if something was dropped
        missing = [k for k, v in contact_vals_raw.items() if v is None]
        if missing:
            print(f"⚠️ Odoo payload had None in: {missing} — removed")

        contact_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "res.partner", "create", [contact_vals]
        )
        print(f"🆕 Created contact ID {contact_id}")
        return contact_id

    except xmlrpc.client.Fault as e:
        print(f"ERROR creating contact (RPC): {e.faultString}")
        return False
    except Exception as e:
        print(f"ERROR creating contact: {e}")
        return False


def _has_text(v) -> bool:
    return isinstance(v, str) and v.strip() != ""

def update_odoo_contact(contact_id, data):
    uid, models = connect_odoo()
    if not uid:
        print("❌ Failed to connect to Odoo for contact update.")
        return False

    # Read current values to make decisions (name/email/phone/mobile/city)
    current = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD, "res.partner", "read",
        [[contact_id]], {"fields": ["name", "email", "phone", "mobile", "city"], "load": "classic"}
    )[0]

    form_name = (data.get("Name") or f"{data.get('First name','')} {data.get('Last name','')}").strip()
    form_email = (data.get("Email") or "").strip().lower()
    form_phone = (data.get("Phone") or "").strip()
    form_city  = (data.get("City") or "").strip()

    update_vals = {}

    # EMAIL: if provided and different, update
    if _has_text(form_email) and form_email != (current.get("email") or "").lower():
        update_vals["email"] = form_email

    # PHONE: if provided and different, update
    if _has_text(form_phone) and form_phone != (current.get("phone") or ""):
        update_vals["phone"] = form_phone

    # CITY: if provided and different, update
    if _has_text(form_city) and form_city != (current.get("city") or ""):
        update_vals["city"] = form_city

    # NAME: update only if safe
    # Safe if existing name empty OR names are similar (variant) OR you matched by exact email
    safe_to_rename = False
    if _has_text(form_name):
        existing_name = current.get("name") or ""
        email_equal = form_email and (form_email == (current.get("email") or "").lower())
        if not existing_name:
            safe_to_rename = True
        elif email_equal and _similar_names(existing_name, form_name):
            safe_to_rename = True
        elif _similar_names(existing_name, form_name):
            safe_to_rename = True
        # else keep existing name; log discrepancy
        if safe_to_rename and form_name != existing_name:
            update_vals["name"] = form_name
        elif not safe_to_rename and form_name and form_name.lower() != existing_name.lower():
            print(f"⚠️ Name mismatch for partner {contact_id!r}: keep '{existing_name}' (form had '{form_name}').")

    # STATE/COUNTRY only if resolvable from the form
    # STATE/COUNTRY only if resolvable from the form
    prov_state_input = (data.get("Prov/State") or data.get("Province/State") or data.get("Privince/State") or "").strip()

    # If your lead data has a Country field, use it as a hint (optional)
    country_hint = (data.get("Country") or "").strip()

    if prov_state_input:
        normalized_state = normalize_state(prov_state_input)

        # 1) Determine country from state code + optional hint
        country_name = resolve_country(prov_state_input, country_hint=country_hint)

        # If ambiguous/unknown, fall back to existing partner country (safer than guessing)
        country_id = False
        if isinstance(country_name, str):
            country_id = get_country_id(models, uid, country_name)

        # 2) Lookup state constrained by country_id (if we have it)
        domain_code = [("code", "=", normalized_state)]
        domain_name = [("name", "ilike", prov_state_input)]

        if country_id:
            domain_code.append(("country_id", "=", country_id))
            domain_name.append(("country_id", "=", country_id))

        state_record = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD, "res.country.state", "search_read",
            [domain_code], {"fields": ["id", "country_id"], "limit": 1},
        ) or models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD, "res.country.state", "search_read",
            [domain_name], {"fields": ["id", "country_id"], "limit": 1},
        )

        if state_record:
            update_vals["state_id"] = state_record[0]["id"]

            # Prefer the inferred country_id if we have one; otherwise fall back to what Odoo returned
            if country_id:
                update_vals["country_id"] = country_id
            elif state_record[0].get("country_id"):
                update_vals["country_id"] = state_record[0]["country_id"][0]


    # TAGS: only set if you actually have tags
    tag_ids = get_or_create_tags(models, uid, data.get("Products Interest", []))
    if tag_ids:
        update_vals["category_id"] = [(6, 0, tag_ids)]

    if not update_vals:
        print(f"ℹ️ Nothing to update for contact {contact_id}.")
        return True

    try:
        ok = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD, "res.partner", "write",
            [[contact_id], update_vals],
        )
        print(f"✅ Contact {contact_id} updated (fields: {list(update_vals.keys())}).")
        return bool(ok)
    except xmlrpc.client.Fault as e:
        print(f"🚨 Odoo RPC Error updating contact {contact_id}: {e.faultString}")
        return False
    except Exception as e:
        print(f"Unexpected error updating contact {contact_id}: {e}")
        return False


def find_existing_contact(data):
    """
    Priority:
      1) email exact (case-insensitive)
      2) phone digits exact in phone OR mobile
      3) name+city fallback with loose similarity
    Returns a dict {id, name, email, phone, mobile, city} or None.
    """
    uid, models = connect_odoo()
    if not uid:
        print("Failed to log in to Odoo for contact search.")
        return None

    email = (data.get("Email") or "").strip().lower()
    phone_norm = _norm_phone(data.get("Phone") or "")
    name_in = (data.get("Name") or f"{data.get('First name','')} {data.get('Last name','')}").strip()
    city = (data.get("City") or "").strip()

    try:
        # 1) Email exact
        if email:
            res = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD, "res.partner", "search_read",
                [[("email", "=", email)]],
                {"fields": ["id", "name", "email", "phone", "mobile", "city"], "limit": 1},
            )
            if res: return res[0]

        # 2) Phone exact
        if phone_norm:
            res = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD, "res.partner", "search_read",
                [['|', ('phone', '!=', False), ('mobile', '!=', False)]],
                {"fields": ["id", "name", "email", "phone", "mobile", "city"], "limit": 50},
            )
            for r in res:
                if _norm_phone(r.get("phone")) == phone_norm or _norm_phone(r.get("mobile")) == phone_norm:
                    return r

        # 3) Name + City fallback
        if name_in and city:
            res = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD, "res.partner", "search_read",
                [["&", ("name", "ilike", name_in), ("city", "ilike", city)]],
                {"fields": ["id", "name", "email", "phone", "mobile", "city"], "limit": 5},
            )
            # choose the one with the most similar name
            best = None
            best_score = -1
            for r in res:
                if _similar_names(r["name"], name_in):
                    # simple score = token overlap size
                    score = len(_name_tokens(r["name"]) & _name_tokens(name_in))
                    if score > best_score:
                        best, best_score = r, score
            if best:
                return best

        return None

    except Exception as e:
        print(f"❌ Error in find_existing_contact: {e}", flush=True)
        traceback.print_exc()
        return None



def create_odoo_opportunity(opportunity_data):
    """
    Creates a new opportunity in Odoo, ensuring proper country/state resolution.
    opportunity_data may include: name, partner_id, city, Prov/State, description, etc.
    """
    uid, models = connect_odoo()
    if not uid:
        print("❌ Failed to connect for opportunity creation.")
        return None

    try:

        # ---------------------------------------------------------
        # State + country resolution (CA/US/AU aware)
        # ---------------------------------------------------------
        prov_state_input = (opportunity_data.get("Prov/State") or opportunity_data.get("State") or "").strip()
        city_value = (opportunity_data.get("city") or opportunity_data.get("City") or "").strip()
        country_hint = (opportunity_data.get("Country") or "").strip()

        state_id = False
        country_id = False

        if prov_state_input:
            normalized_state = normalize_state(prov_state_input)
            country_name = resolve_country(prov_state_input, country_hint=country_hint)
            if isinstance(country_name, str):
                country_id = get_country_id(models, uid, country_name)

            domain_code = [("code", "=", normalized_state)]
            domain_name = [("name", "ilike", prov_state_input)]
            if country_id:
                domain_code.append(("country_id", "=", country_id))
                domain_name.append(("country_id", "=", country_id))

            state_record = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "res.country.state", "search_read",
                [domain_code], {"fields": ["id", "country_id"], "limit": 1},
            ) or models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "res.country.state", "search_read",
                [domain_name], {"fields": ["id", "country_id"], "limit": 1},
            )

            if state_record:
                state_id = state_record[0]["id"]
                if not country_id and state_record[0].get("country_id"):
                    country_id = state_record[0]["country_id"][0]

        # ---------------------------------------------------------------------
        # Build opportunity payload (avoid mutating caller dict)
        # ---------------------------------------------------------------------
        opportunity_vals = dict(opportunity_data)
        opportunity_vals.pop("Prov/State", None)
        opportunity_vals.pop("State", None)
        if city_value:
            opportunity_vals["city"] = city_value
        if state_id:
            opportunity_vals["state_id"] = state_id
        if country_id:
            opportunity_vals["country_id"] = country_id

        new_opportunity_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "crm.lead", "create",
            [opportunity_vals],
        )
        print(f"✅ Opportunity created with ID: {new_opportunity_id}")
        return new_opportunity_id

    except xmlrpc.client.Fault as fault:
        print(
            f"🚨 Odoo RPC Error creating opportunity: Code={fault.faultCode}, Message={fault.faultString}"
        )
        return None
    except Exception as e:
        print("❌ Unexpected error creating opportunity in Odoo:", flush=True)
        traceback.print_exc()
        return None



def find_odoo_user_id(models, uid, user_name):
    """
    Finds the Odoo user ID by name.
    """
    try:
        user = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            'res.users', 'search_read',
            [[('name', '=', user_name)]], {'fields': ['id'], 'limit': 1})
        if user:
            print(f"DEBUG (connector): Found user '{user_name}' with ID: {user[0]['id']}")
            return user[0]['id']
        else:
            print(f"DEBUG (connector): User '{user_name}' not found in Odoo.")
            return False
    except xmlrpc.client.Fault as e:
        print(f"Odoo RPC Error finding user '{user_name}': {e.faultString}")
        return False
    except Exception as e:
        print(f"Unexpected error finding user '{user_name}': {e}")
        return False

def create_odoo_activity_via_message(models, uid, opportunity_id, user_id, summary, note):
    """
    Creates an activity linked to an opportunity using activity_schedule().
    Works on Odoo SaaS — no ir.model access required.
    """
    try:
        # Get 'To-Do' activity type ID
        activity_type = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "mail.activity.type", "search_read",
            [[("name", "=", "To-Do")]],
            {"fields": ["id"], "limit": 1},
        )
        activity_type_id = activity_type[0]["id"] if activity_type else 1

        # Schedule the activity directly (no need for res_model_id)
        result = models.execute_kw(
            ODOO_DB,
            uid,
            ODOO_PASSWORD,
            "crm.lead",
            "activity_schedule",
            [[opportunity_id]],  # record(s)
            {
                "activity_type_id": activity_type_id,
                "summary": summary,
                "note": note,
                "user_id": user_id,
                "date_deadline": datetime.now().strftime("%Y-%m-%d"),
            },
        )

        print(f"✅ Activity scheduled via activity_schedule: {result}")
        return result

    except xmlrpc.client.Fault as fault:
        print(f"Odoo RPC Error scheduling activity: Code={fault.faultCode}, Message={fault.faultString}")
        return False
    except Exception as e:
        print(f"Unexpected error scheduling activity: {e}")
        return False

from datetime import date

from datetime import date  # make sure this import exists

def schedule_activity_for_lead(models, uid, lead_id, user_id, summary, note, deadline_date=None):
    """
    Create a mail.activity on a lead without querying ir.model.
    Strategy:
      1) Try res_model='crm.lead' (works on many Odoo 17 SaaS instances)
      2) If server requires res_model_id, retry if CRM_LEAD_MODEL_ID is set
      3) Otherwise post an internal note as a safe fallback
    """
    if not deadline_date:
        deadline_date = date.today().strftime("%Y-%m-%d")

    # Get To-Do type id
    todo = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "mail.activity.type", "search_read",
        [[("name", "=", "To-Do")]],
        {"fields": ["id"], "limit": 1},
    )
    activity_type_id = int(todo[0]["id"]) if todo else 1

    # Attempt 1: use res_model (no ir.model permission needed)
    vals_res_model = {
        "activity_type_id": activity_type_id,
        "res_model": "crm.lead",
        "res_id": int(lead_id),
        "user_id": int(user_id),
        "summary": summary or "",
        "note": note or "",
        "date_deadline": deadline_date,
    }
    try:
        activity_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "mail.activity", "create", [vals_res_model]
        )
        print(f"🗓️ Activity created (mail.activity id={activity_id}) for opportunity {lead_id}")
        return activity_id
    except Exception as e:
        msg = getattr(e, "faultString", str(e)) or ""
        # Some instances mandate res_model_id
        needs_id = "res_model_id" in msg.lower() or "document model" in msg.lower()

    # Attempt 2: retry with res_model_id if available
    if 'needs_id' in locals() and needs_id and isinstance(globals().get("CRM_LEAD_MODEL_ID"), int):
        vals_res_model_id = {
            "activity_type_id": activity_type_id,
            "res_model_id": int(CRM_LEAD_MODEL_ID),  # set this once per DB if you have it
            "res_id": int(lead_id),
            "user_id": int(user_id),
            "summary": summary or "",
            "note": note or "",
            "date_deadline": deadline_date,
        }
        activity_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "mail.activity", "create", [vals_res_model_id]
        )
        print(f"🗓️ Activity created with res_model_id (mail.activity id={activity_id}) for opportunity {lead_id}")
        return activity_id

    # Attempt 3: final fallback, post an internal note so the webhook still succeeds
    print("⚠️ Could not create activity (res_model or res_model_id). Posting a note as fallback.")
    try:
        models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "crm.lead", "message_post",
            [[int(lead_id)]],
            {
                "body": (note or summary or "Follow-up"),
                "message_type": "comment",
                "subtype_xmlid": "mail.mt_note",
            },
        )
        print(f"📝 Posted note on opportunity {lead_id} (activity fallback).")
    except Exception as e2:
        print(f"❌ Failed to post note fallback: {e2}")
    return False

    
def find_existing_opportunity(opportunity_name):
    """
    Finds an existing opportunity in Odoo by its name.
    Returns a dictionary of the opportunity's info (id, name) or None.
    Handles extra spaces, Unicode whitespace, trailing spaces, and case differences.
    Returns the closest match if no exact match is found.
    """
    def normalize_spaces(text: str) -> str:
        """
        Normalize all whitespace:
        - Strip leading/trailing whitespace
        - Collapse any sequence of whitespace (including non-breaking) into a single space
        """
        text = ''.join(c for c in text if c in string.printable)
        # Replace any whitespace sequence with a single space
        text = re.sub(r'\s+', ' ', text.strip())
        return text.lower()
    
    # Normalize the input name
    target_name = normalize_spaces(opportunity_name)
    
    uid, models = connect_odoo()
    if not uid:
        print("Failed to log in to Odoo for opportunity search.")
        return None
    
    try:
        # Search using ilike (case-insensitive, substring)
        domain = [('name', 'ilike', target_name)]
        existing = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            'crm.lead', 'search_read',
            [domain],
            {'fields': ['id', 'name']}
        )
        
        closest_match = None
        
        for record in existing:
            record_name = normalize_spaces(record['name'])
            
            # Exact match (after normalization)
            if record_name.lower() == target_name.lower():
                print(f"DEBUG: Exact match found: {record_name} (ID: {record['id']})")
                return record
            
            # Save the first record as closest match if exact not found
            if closest_match is None:
                closest_match = record
        
        if closest_match:
            print(f"DEBUG: No exact match found. Returning closest match: {closest_match['name']} (ID: {closest_match['id']})")
            return closest_match
        
        print(f"DEBUG: No opportunity found matching: '{opportunity_name}'")
        return None

    except xmlrpc.client.Fault as e:
        print(f"Odoo RPC Error finding opportunity '{opportunity_name}': {e.faultString}")
        return None
    except Exception as e:
        print(f"Unexpected error finding opportunity '{opportunity_name}': {e}")
        return None



def update_odoo_opportunity(opportunity_id, opportunity_data):
    """
    Updates an existing opportunity in Odoo.
    opportunity_data is a dictionary containing fields to update.
    """
    uid, models = connect_odoo()
    if not uid:
        print("Failed to log in to Odoo for opportunity update.")
        return False
    try:
        success = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            'crm.lead',
            'write',
            [[opportunity_id], opportunity_data]
        )
        print(f"Opportunity ID {opportunity_id} updated: {success}")
        return success
    except xmlrpc.client.Fault as fault:
        print(f"Odoo RPC Error updating opportunity {opportunity_id}: Code={fault.faultCode}, Message={fault.faultString}")
        return False
    except Exception as e:
        print(f"Unexpected error updating opportunity {opportunity_id}: {e}")
        return False
    
def post_internal_note_to_opportunity(models, uid, opportunity_id, note_content):
    """
    Posts an internal note to a specific Odoo opportunity.

    :param models: The Odoo models proxy object.
    :param uid: The user ID for authentication.
    :param opportunity_id: The ID of the CRM opportunity (crm.lead) to update.
    :param note_content: The text of the note to post.
    :return: The ID of the new message, or False on failure.
    """
    try:
        message_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "crm.lead",
            "message_post",
            [[opportunity_id]],
            {
                "body": note_content,
                "message_type": "comment",
                "subtype_xmlid": "mail.mt_note",
            }
        )
        print(f"INFO: Successfully posted internal note to opportunity {opportunity_id}.")
        return message_id
    except Exception as e:
        print(f"ERROR: Failed to post internal note to opportunity {opportunity_id}: {e}")
        return False
