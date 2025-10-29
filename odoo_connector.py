# -----------------------------
# file: odoo_connector.py
# -----------------------------
import xmlrpc.client
from datetime import datetime, date, timedelta 
import math
from typing import Optional, Union
import re
import string
import traceback

ODOO_URL = 'https://wavcor-international-inc2.odoo.com'
#ODOO_URL = 'https://wavcor-test-2025-07-20.odoo.com'
ODOO_DB = 'wavcor-international-inc2'
#ODOO_DB = 'wavcor-test-2025-07-20'
#ODOO_USERNAME = 'jason@wavcor.ca'
#ODOO_PASSWORD = 'Wavcor3702?'
ODOO_USERNAME = 'al@wavcor.ca'
ODOO_PASSWORD = 'wavcor3702'
CRM_LEAD_MODEL_ID = 1082

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
    {"Location": "Mariapolis Agro Centre", "Latitude": 49.360666, "Longitude": -98.98952, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Baldur Agro Centre", "Latitude": 49.385578, "Longitude": -99.24384, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Glenboro Agro Centre", "Latitude": 49.555833, "Longitude": -99.291111, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Minto Agro Centre", "Latitude": 49.4312, "Longitude": -100.2227, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Manitou Agro Centre", "Latitude": 49.240555, "Longitude": -98.536667, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Kyle Agro Centre", "Latitude": 50.8327023, "Longitude": -108.0392125, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Frontier Agro Centre", "Latitude": 49.204894, "Longitude": -108.561809, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Maple Creek Agro Centre", "Latitude": 49.8, "Longitude": -109.1, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Gull Lake Agro Centre & Gas Bar", "Latitude": 50.1, "Longitude": -108.4, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Cabri Agro Centre", "Latitude": 50.62, "Longitude": -108.46, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Herbert Farm Centre & Gas Bar", "Latitude": 50.4275233, "Longitude": -107.2234377, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Morse Farm Centre", "Latitude": 50.33425, "Longitude": -106.9663, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Sceptre Farm Centre", "Latitude": 50.86281, "Longitude": -109.27075, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Ponteix Farm Centre", "Latitude": 49.74138, "Longitude": -107.469433, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Swift Current Agro", "Latitude": 50.285765, "Longitude": -107.851187, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Shaunavon Home & Agro Centre", "Latitude": 49.644498, "Longitude": -108.415893, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Eastend Agro Centre", "Latitude": 49.51, "Longitude": -108.82, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Hazlet Agro Centre", "Latitude": 50.4000622, "Longitude": -108.5939493, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Consul Agro Centre & Food Store", "Latitude": 49.2953781, "Longitude": -109.5201135, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Tompkins Farm Centre", "Latitude": 50.067518, "Longitude": -108.805208, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Abbey Farm Centre", "Latitude": 50.7369, "Longitude": -108.7575, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Wiseton Farm Supply", "Latitude": 51.315073, "Longitude": -107.650071, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Beechy Farm Supply", "Latitude": 50.8793278, "Longitude": -107.3838575, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Outlook Home & Agro", "Latitude": 51.48866, "Longitude": -107.05039, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Davidson Home Agro & Liquor", "Latitude": 51.2628546, "Longitude": -105.9890338, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Broderick Agro Centre", "Latitude": 51.592881, "Longitude": -106.9175, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Strongfield Agro Centre", "Latitude": 51.33159, "Longitude": -106.58952, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Tullis Agro Centre", "Latitude": 51.038626, "Longitude": -107.037085, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Saskatoon Coop", "Latitude": 52.1332, "Longitude": -106.67, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Hepburn Ag Centre", "Latitude": 52.524511, "Longitude": -106.731207, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Colonsay Ag Centre", "Latitude": 51.980557, "Longitude": -105.86921, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Watrous Ag Centre", "Latitude": 51.676963, "Longitude": -105.483289, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Norquay Coop", "Latitude": 51.9833, "Longitude": -102.35, "Contact": "Gerald Fehr", "Phone": "306-594-2215"},
    {"Location": "Wynyard Coop", "Latitude": 51.7833, "Longitude": -104.1667, "Contact": "Victor Hawryluk", "Phone": "306-874-7816"},
    {"Location": "Turtleford", "Latitude": 53.034, "Longitude": -108.973, "Contact": "Kelly Svoboda", "Phone": "306-845-2183"},
    {"Location": "Maidstone Coop", "Latitude": 53.081224, "Longitude": -109.2957338, "Contact": "Kelly Svoboda", "Phone": "306-845-2183"},
    {"Location": "Bulyea Co-Op", "Latitude": 50.987541, "Longitude": -104.865293, "Contact": "Brad Foster", "Phone": "(306) 725-4931"},
    {"Location": "Luseland Home & Agro Centre", "Latitude": 52.079015, "Longitude": -109.39177, "Contact": "Michael Kwiatkowski", "Phone": "306.228.2624"},
    {"Location": "Unity Agro Centre & Bulk Petroleum", "Latitude": 52.439538, "Longitude": -109.153921, "Contact": "Michael Kwiatkowski", "Phone": "306.228.2624"},
    {"Location": "Macklin Agro Centre", "Latitude": 52.329738, "Longitude": -109.94157, "Contact": "Michael Kwiatkowski", "Phone": "306.228.2624"},
    {"Location": "Lloydminster Agro Centre", "Latitude": 53.268327, "Longitude": -109.964267, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Lloyd South Agro Centre", "Latitude": 53.268327, "Longitude": -109.964267, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Lashburn Agro Centre", "Latitude": 53.124683, "Longitude": -109.617828, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Neilburg Agro & Lumber Centre", "Latitude": 52.841832, "Longitude": -109.624412, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Hudson Bay Agro Centre", "Latitude": 52.857415, "Longitude": -102.391506, "Contact": "Karissa Lupuliak", "Phone": "(306) 865-2288"},
    {"Location": "St. Isidore Agro Centre & Food Store", "Latitude": 56.206934, "Longitude": -117.108478, "Contact": "Jeff Labrecque", "Phone": "(780) 624-3121"},
    {"Location": "Falher Agro Centre", "Latitude": 55.738632, "Longitude": -117.198337, "Contact": "Jeff Labrecque", "Phone": "(780) 624-3121"},
    {"Location": "Plenty Agro Centre", "Latitude": 51.782722, "Longitude": -108.645328, "Contact": "Scott Burton", "Phone": "306-932-7072"},
    {"Location": "Rosetown Agro Centre", "Latitude": 51.547969, "Longitude": -108.001044, "Contact": "Scott Burton", "Phone": "306-932-7072"},
    {"Location": "Landis Agro Centre", "Latitude": 52.201375, "Longitude": -108.459017, "Contact": "Scott Burton", "Phone": "306-932-7072"},
    {"Location": "Naicam Agro Centre", "Latitude": 52.377063, "Longitude": -104.499982, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Melfort Home & Agro Centre", "Latitude": 52.866642, "Longitude": -104.635423, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Kelvington Home & Agro Centre", "Latitude": 52.164223, "Longitude": -103.525442, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Archerwill Agro Centre", "Latitude": 52.441637, "Longitude": -103.864246, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Lake Lenore Agro Centre", "Latitude": 52.395224, "Longitude": -104.979865, "Contact": "Liam Jennett", "Phone": "(306) 368-2255"},
    {"Location": "Tisdale Agro Centre", "Latitude": 52.841191, "Longitude": -104.052337, "Contact": "Tammy Doerksen", "Phone": "(306) 873-5111"},
    {"Location": "Carrot River Farm Supply", "Latitude": 53.281467, "Longitude": -103.584011, "Contact": "Tammy Doerksen", "Phone": "306.873.5111"},
    {"Location": "Kindersley Co-op Coleville Cardlock", "Latitude": 51.705463, "Longitude": -109.240564, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Kerrobert Cardlock", "Latitude": 51.917593, "Longitude": -109.135279, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Brock Cardlock", "Latitude": 51.439892, "Longitude": -108.720097, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Hoosier Cardlock", "Latitude": 51.624495, "Longitude": -109.739751, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Marengo Cardlock", "Latitude": 51.477654, "Longitude": -109.782534, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Eatonia Cardlock", "Latitude": 51.221243, "Longitude": -109.385064, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Cardlock", "Latitude": 51.470174, "Longitude": -109.143149, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Eatonia Farm Supply and", "Latitude": 51.221001, "Longitude": -109.388583, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Parkland Co-Op", "Latitude": 52.599479, "Longitude": -103.251540, "Contact": "Don Hilash", "Phone": "306-278-3113"},
    {"Location": "Hudson Bay Agro Centre", "Latitude": 52.85833, "Longitude": -102.38861, "Contact": "Karissa Lupuliak", "Phone": "306-865-2288"}
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

def find_closest_dealer(customer_lat, customer_lon):
    """
    Finds the closest dealer from the predefined DEALER_LOCATIONS.
    Returns a dictionary of the closest dealer's info or None if no dealers.
    """
    if not DEALER_LOCATIONS:
        print("No dealer locations defined.")
        return None

    closest_dealer = None
    min_distance = float('inf') # Initialize with a very large number

    for dealer in DEALER_LOCATIONS:
        dealer_lat = dealer["Latitude"]
        dealer_lon = dealer["Longitude"]
        
        distance = haversine_distance(customer_lat, customer_lon, dealer_lat, dealer_lon)
        
        if distance < min_distance:
            min_distance = distance
            closest_dealer = dealer
            closest_dealer["Distance_km"] = round(distance, 2) # Add distance to the result

    return closest_dealer


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

        # --- Normalize and find the state ---
        normalized_state = normalize_state(data.get("Prov/State", ""))
        #print(f"DEBUG: Normalized state is '{normalized_state}'.")

        state_id = False
        country_id = False

        if normalized_state:
            state_record = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "res.country.state", "search_read",
                [[("code", "=", normalized_state)]],
                {"fields": ["id", "country_id"], "limit": 1},
            )
            if state_record:
                state_id = state_record[0]["id"]
                #print(f"DEBUG: Found Odoo state ID {state_id} by code for '{normalized_state}'.")
                # Also get the linked country ID from that state
                if state_record[0].get("country_id"):
                    country_id = state_record[0]["country_id"][0]
                    print(f"DEBUG: Country ID derived from state: {country_id}")
            else:
                print(f"DEBUG: State '{normalized_state}' not found in Odoo.")

        # --- Fallback country logic ---
        if not country_id:
            # Try to set Canada by default (or adapt to your region)
            country = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "res.country", "search_read",
                [[("code", "=", "CA")]],
                {"fields": ["id"], "limit": 1},
            )
            if country:
                country_id = country[0]["id"]
                #print(f"DEBUG: Defaulted to Canada (Country ID: {country_id})")
            else:
                print("DEBUG: Could not find country 'CA' in Odoo — leaving blank.")

        #print(f"DEBUG: Country ID determined for contact: {country_id}")

        # --- Build the contact payload ---
        contact_vals_raw = {
            "name": name_val,
            "email": _ensure_char(data.get("Email")),
            "phone": _ensure_char(data.get("Phone")),
            "city": _ensure_char(data.get("City")),
            "state_id": _ensure_id(state_id) or False,
            "country_id": _ensure_id(country_id) or False,
        }

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
    prov_state_input = (data.get("Prov/State") or data.get("Province/State") or data.get("Privince/State") or "").strip()
    if prov_state_input:
        normalized_state = normalize_state(prov_state_input)
        state_record = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD, "res.country.state", "search_read",
            [[("code", "=", normalized_state)]], {"fields": ["id", "country_id"], "limit": 1},
        ) or models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD, "res.country.state", "search_read",
            [[("name", "ilike", prov_state_input)]], {"fields": ["id", "country_id"], "limit": 1},
        )
        if state_record:
            update_vals["state_id"] = state_record[0]["id"]
            if state_record[0].get("country_id"):
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
                [[("|", ("phone", "!=", False), ("mobile", "!=", False))]],
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
        # ---------------------------------------------------------------------
        # Province/state normalization
        # ---------------------------------------------------------------------
        prov_state_input = (opportunity_data.get("Prov/State") or "").strip()
        city_value = opportunity_data.get("city") or opportunity_data.get("City") or ""
        state_id = False
        country_id = False

        if prov_state_input:
            normalized_state = normalize_state(prov_state_input)
            #print(f"DEBUG: In create_odoo_opportunity Prov/State is '{prov_state_input}', normalized to '{normalized_state}'.")

            state_record = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "res.country.state", "search_read",
                [[("code", "=", normalized_state)]],
                {"fields": ["id", "country_id"], "limit": 1},
            )

            if state_record:
                state_id = state_record[0]["id"]
                #print(f"DEBUG: Found Odoo state ID {state_id} for '{normalized_state}'.")
                if state_record[0].get("country_id"):
                    country_id = state_record[0]["country_id"][0]
                    print(f"DEBUG: Derived country ID {country_id} from state.")
            else:
                print(f"DEBUG: State '{normalized_state}' not found in Odoo.")

        # ---------------------------------------------------------------------
        # Fallback: use Canada if country still not set
        # ---------------------------------------------------------------------
        if not country_id:
            country = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "res.country", "search_read",
                [[("code", "=", "CA")]],
                {"fields": ["id"], "limit": 1},
            )
            if country:
                country_id = country[0]["id"]
                #print(f"DEBUG: Defaulted to Canada (Country ID: {country_id}).")
            else:
                print("DEBUG: Could not find country 'CA' in Odoo — leaving blank.")

        #print(f"DEBUG: Final country_id={country_id}, state_id={state_id}")

        # ---------------------------------------------------------------------
        # Merge IDs into opportunity data
        # ---------------------------------------------------------------------
        opportunity_data["city"] = city_value
        opportunity_data["state_id"] = state_id or False
        opportunity_data["country_id"] = country_id or False

        # ---------------------------------------------------------------------
        # Create the opportunity
        # ---------------------------------------------------------------------
        opportunity_data.pop("Prov/State", None)
        new_opportunity_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "crm.lead", "create",
            [opportunity_data],
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



# --- NEW FUNCTION: Find Odoo User ID ---
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

def schedule_activity_for_lead(models, uid, lead_id, user_id, summary, note, deadline_date=None):
    """
    Create a mail.activity on a lead without reading ir.model (no extra perms).
    Requires CRM_LEAD_MODEL_ID to be set to the 'crm.lead' ir.model id.
    """
    if not deadline_date:
        deadline_date = date.today().strftime("%Y-%m-%d")

    # 1) Get To-Do type (safe)
    todo = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "mail.activity.type", "search_read",
        [[("name", "=", "To-Do")]],
        {"fields": ["id"], "limit": 1},
    )
    activity_type_id = int(todo[0]["id"]) if todo else 1

    # 2) Require the model id (no ir.model read)
    if not isinstance(CRM_LEAD_MODEL_ID, int):
        # Fall back to an internal note so the webhook still succeeds
        print("⚠️ CRM_LEAD_MODEL_ID not set; posting note instead of scheduling activity.")
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
            print(f"📝 Posted note on opportunity {lead_id} (no activity, missing CRM_LEAD_MODEL_ID).")
        except Exception as e:
            print(f"❌ Failed to post note fallback: {e}")
        return False

    vals = {
        "activity_type_id": activity_type_id,
        "res_model_id": int(CRM_LEAD_MODEL_ID),  # <-- no ir.model lookup
        "res_id": int(lead_id),
        "user_id": int(user_id),
        "summary": summary or "",
        "note": note or "",
        "date_deadline": deadline_date,
    }

    activity_id = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "mail.activity", "create", [vals]
    )
    print(f"🗓️ Activity created (mail.activity id={activity_id}) for opportunity {lead_id}")
    return activity_id



# cache to avoid repeated ir.model lookups
_CRM_LEAD_MODEL_ID = None

def schedule_activity_for_lead(models, uid, lead_id, user_id, summary, note, deadline_date=None):
    """
    Create a mail.activity for a lead in Odoo 17 SaaS via XML-RPC.
    Uses res_model_id (mandatory) instead of res_model to avoid server complaints.
    Returns the integer activity ID.
    """
    from datetime import date
    global _CRM_LEAD_MODEL_ID

    # 1) Resolve mail.activity.type (To-Do)
    todo = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "mail.activity.type", "search_read",
        [[("name", "=", "To-Do")]],
        {"fields": ["id"], "limit": 1},
    )
    activity_type_id = todo[0]["id"] if todo else 1

    # 2) Resolve res_model_id for crm.lead (cache it)
    if not _CRM_LEAD_MODEL_ID:
        model_rec = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "ir.model", "search_read",
            [[("model", "=", "crm.lead")]],
            {"fields": ["id"], "limit": 1},
        )
        if not model_rec:
            raise RuntimeError("Cannot find ir.model for crm.lead")
        _CRM_LEAD_MODEL_ID = model_rec[0]["id"]

    # 3) Build XML-RPC safe values
    if not deadline_date:
        deadline_date = date.today().strftime("%Y-%m-%d")

    vals = {
        "activity_type_id": int(activity_type_id),
        "res_model_id": int(_CRM_LEAD_MODEL_ID),  # ← mandatory
        "res_id": int(lead_id),
        "user_id": int(user_id),
        "summary": summary or "",
        "note": note or "",
        "date_deadline": deadline_date,           # 'YYYY-MM-DD'
    }

    activity_id = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "mail.activity", "create", [vals]
    )
    print(f"🗓️ Activity created (mail.activity id={activity_id}) for opportunity {lead_id}")
    return activity_id

    
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