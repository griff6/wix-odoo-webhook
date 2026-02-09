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
ODOO_USERNAME = 'sales@wavcor.ca'
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
    {"Location": "BORDERLAND CO-OPERATIVE LTD (Broadview Agro Centre)", "Latitude": 50.378923, "Longitude": -102.583995, "Contact": "Aart Kohler", "Phone": "(306) 696-3038"},
    {"Location": "Camrose Agro Centre", "Latitude": 52.910438, "Longitude": -112.726688, "Contact": "Adam Creasy", "Phone": "780.385.5877"},
    {"Location": "Sedgewick Agro Centre", "Latitude": 52.776455, "Longitude": -111.695401, "Contact": "Adam Creasy", "Phone": "780.385.5877"},
    {"Location": "Tofield Agro Centre", "Latitude": 53.369913, "Longitude": -112.668654, "Contact": "Adam Creasy", "Phone": "780.385.5877"},
    {"Location": "Viking Agro Centre", "Latitude": 53.092599, "Longitude": -111.777788, "Contact": "Adam Creasy", "Phone": "780.385.5877"},
    {"Location": "Marquette Agro Centre", "Latitude": 50.064485, "Longitude": -97.734061, "Contact": "Bob", "Phone": "204-781-2447"},
    {"Location": "Starbuck Agro Centre", "Latitude": 49.768476, "Longitude": -97.618268, "Contact": "Bob", "Phone": "204-781-2447"},
    {"Location": "BULYEA CO-OP", "Latitude": 50.986173, "Longitude": -104.86599, "Contact": "Brad Foster", "Phone": "(306) 725-4931"},
    {"Location": "Kindersley Co-op Brock Cardlock", "Latitude": 51.441207, "Longitude": -108.717969, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Coleville Cardlock", "Latitude": 51.710641, "Longitude": -109.245206, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Eatonia Farm Supply and", "Latitude": 51.224182, "Longitude": -109.390036, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Hoosier Cardlock", "Latitude": 51.625239, "Longitude": -109.73855, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Kindersley Co-op Marengo Cardlock", "Latitude": 51.480717, "Longitude": -109.780565, "Contact": "Brent Jones", "Phone": "306-460-5717"},
    {"Location": "Hanna Agro Centre", "Latitude": 51.644557, "Longitude": -111.927479, "Contact": "Brian Sortland", "Phone": "403.507.0624"},
    {"Location": "Westview Co-op Consort", "Latitude": 52.00825, "Longitude": -110.76512, "Contact": "Brian Sortland", "Phone": "403.507.0624"},
    {"Location": "Lashburn Agro Centre", "Latitude": 53.136241, "Longitude": -109.628509, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Lloyd South Agro Centre", "Latitude": 53.281667, "Longitude": -109.986339, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Neilburg Agro & Lumber Centre", "Latitude": 52.83766, "Longitude": -109.627995, "Contact": "Chad Gessner", "Phone": "(306) 825-8180"},
    {"Location": "Norquay Coop", "Latitude": 51.9833, "Longitude": -102.35, "Contact": "Gerald Fehr", "Phone": "306-594-2215"},
    {"Location": "Archerwill Agro Centre", "Latitude": 52.439137, "Longitude": -103.863044, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Kelvington Home & Agro Centre", "Latitude": 52.164397, "Longitude": -103.522808, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Melfort Home & Agro Centre", "Latitude": 52.861371, "Longitude": -104.613554, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Naicam Agro Centre", "Latitude": 52.416507, "Longitude": -104.496329, "Contact": "Jason Hutchingson", "Phone": "306-980-6770"},
    {"Location": "Estevan Bulk & Agro", "Latitude": 49.142808, "Longitude": -102.991075, "Contact": "Jason Macdonald", "Phone": "(306) 637-4330"},
    {"Location": "Falher Agro Centre", "Latitude": 55.734688, "Longitude": -117.201276, "Contact": "Jeff Labreque", "Phone": "780-625-9043"},
    {"Location": "St. Isidore Agro Centre & Food Store", "Latitude": 56.205507, "Longitude": -117.104895, "Contact": "Jeff Labreque", "Phone": "780-625-9043"},
    {"Location": "Canwood Agro Centre", "Latitude": 53.358562, "Longitude": -106.600687, "Contact": "Jim Norish", "Phone": "(306)862-4595"},
    {"Location": "Kinistino Agro Centre", "Latitude": 52.952775, "Longitude": -105.029297, "Contact": "Jim Norish", "Phone": "(306)862-4595"},
    {"Location": "Prince Albert Agro Centre", "Latitude": 53.201097, "Longitude": -105.748901, "Contact": "Jim Norish", "Phone": "(306)862-4595"},
    {"Location": "Shellbrook Agro Centre", "Latitude": 53.22248, "Longitude": -106.387482, "Contact": "Jim Norish", "Phone": "(306)862-4595"},
    {"Location": "Spiritwood Farm Supply", "Latitude": 53.365233, "Longitude": -107.517502, "Contact": "Jim Norish", "Phone": "(306)862-4595"},
    {"Location": "Rolla Agro Centre", "Latitude": 55.897979, "Longitude": -120.139873, "Contact": "John Currie", "Phone": "(250) 759-4770"},
    {"Location": "Maidstone Coop", "Latitude": 53.081224, "Longitude": -109.295734, "Contact": "Kelly Svoboda", "Phone": "306-845-2183"},
    {"Location": "Turtleford", "Latitude": 53.034, "Longitude": -108.973, "Contact": "Kelly Svoboda", "Phone": "306-845-2183"},
    {"Location": "Crossfield Agro Centre", "Latitude": 51.427274, "Longitude": -114.030994, "Contact": "Kevin Latimer", "Phone": "403-586-1452"},
    {"Location": "Eckville Farm Centre", "Latitude": 52.363265, "Longitude": -114.36273, "Contact": "Kevin Latimer", "Phone": "403-586-1452"},
    {"Location": "Innisfail Agro Centre", "Latitude": 52.027465, "Longitude": -113.950235, "Contact": "Kevin Latimer", "Phone": "403-586-1452"},
    {"Location": "Lacombe Agro Centre", "Latitude": 52.472752, "Longitude": -113.733215, "Contact": "Kevin Latimer", "Phone": "403-586-1452"},
    {"Location": "Spruce View Farm Centre", "Latitude": 52.085722, "Longitude": -114.3106, "Contact": "Kevin Latimer", "Phone": "403-586-1452"},
    {"Location": "Stettler Agro Centre", "Latitude": 52.322875, "Longitude": -112.71302, "Contact": "Kevin Latimer", "Phone": "403-586-1452"},
    {"Location": "Lake Lenore Agro Centre", "Latitude": 52.396159, "Longitude": -104.983562, "Contact": "Liam Jennett", "Phone": "(306) 368-2255"},
    {"Location": "GILBERT PLAINS CO-OP", "Latitude": 51.149067, "Longitude": -100.488638, "Contact": "Lyle Gouldsborough", "Phone": "204-548-2099"},
    {"Location": "Leduc Co-op Agro Centre", "Latitude": 53.26127, "Longitude": -113.811426, "Contact": "Marcia Williams", "Phone": "780-239-7560"},
    {"Location": "Leduc Home & Agro Centre", "Latitude": 53.260782, "Longitude": -113.551168, "Contact": "Marcia Williams", "Phone": "780-239-7560"},
    {"Location": "Luseland Home & Agro Centre", "Latitude": 52.082266, "Longitude": -109.390962, "Contact": "Michael Kwiatkowski", "Phone": "306.228.2624"},
    {"Location": "Macklin Agro Centre", "Latitude": 52.32659, "Longitude": -109.93661, "Contact": "Michael Kwiatkowski", "Phone": "306.228.2624"},
    {"Location": "Unity Agro Centre & Bulk Petroleum", "Latitude": 52.442851, "Longitude": -109.154899, "Contact": "Michael Kwiatkowski", "Phone": "306.228.2624"},
    {"Location": "Avonlea Home & Agro Centre", "Latitude": 50.014587, "Longitude": -105.055646, "Contact": "Mike Heistad", "Phone": "(306) 868-2133"},
    {"Location": "Moose Jaw Agro Centre", "Latitude": 50.396721, "Longitude": -105.535666, "Contact": "Mike Heistad", "Phone": "(306) 868-2133"},
    {"Location": "Landis Agro Centre", "Latitude": 52.2, "Longitude": -108.45, "Contact": "Scott Burton", "Phone": "306-932-7072"},
    {"Location": "Plenty Agro Centre", "Latitude": 51.782889, "Longitude": -108.647739, "Contact": "Scott Burton", "Phone": "306-932-7072"},
    {"Location": "Rosetown Agro Centre", "Latitude": 51.554815, "Longitude": -107.991286, "Contact": "Scott Burton", "Phone": "306-932-7072"},
    {"Location": "Baldur Agro Centre", "Latitude": 49.385578, "Longitude": -99.24384, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Glenboro Agro Centre", "Latitude": 49.555833, "Longitude": -99.291111, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Manitou Agro Centre", "Latitude": 49.240555, "Longitude": -98.536667, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Mariapolis Agro Centre", "Latitude": 49.360666, "Longitude": -98.98952, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "Minto Agro Centre", "Latitude": 49.4312, "Longitude": -100.2227, "Contact": "Scott Hainsworth", "Phone": "204-723-0249"},
    {"Location": "St. Leon Agro Centre", "Latitude": 49.362883, "Longitude": -98.590628, "Contact": "Scott Hainsworth", "Phone": "(204) 744-2850"},
    {"Location": "Carrot River Farm Supply", "Latitude": 53.284101, "Longitude": -103.584595, "Contact": "Tammy Doerksen", "Phone": "P: 306.873.5111"},
    {"Location": "Tisdale Agro Centre", "Latitude": 52.850059, "Longitude": -104.048767, "Contact": "Tammy Doerksen", "Phone": "(306) 873-5111"},
    {"Location": "Thorhild Home & Agro Centre", "Latitude": 54.250782, "Longitude": -113.057067, "Contact": "Thomas Pelletier", "Phone": "(780) 398-3975"},
    {"Location": "Drumheller Home & Agro Centre", "Latitude": 51.416933, "Longitude": -112.640238, "Contact": "Todd Bossert", "Phone": "403.854.8420"},
    {"Location": "Beechy Farm Supply", "Latitude": 50.879328, "Longitude": -107.383858, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Broderick Agro Centre", "Latitude": 51.592881, "Longitude": -106.9175, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Davidson Home Agro & Liquor", "Latitude": 51.262855, "Longitude": -105.989034, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Outlook Home & Agro", "Latitude": 51.48866, "Longitude": -107.05039, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Strongfield Agro Centre", "Latitude": 51.33159, "Longitude": -106.58952, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Tullis Agro Centre", "Latitude": 51.038626, "Longitude": -107.037085, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Wiseton Farm Supply", "Latitude": 51.315073, "Longitude": -107.650071, "Contact": "Tony Britnell", "Phone": "306-867-7672"},
    {"Location": "Wynyard Agro Centre", "Latitude": 51.7833, "Longitude": -104.1667, "Contact": "Victor Hawryluk", "Phone": "306-874-7816"},
    {"Location": "Colonsay Ag Centre", "Latitude": 51.980557, "Longitude": -105.86921, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Hepburn Ag Centre", "Latitude": 52.524511, "Longitude": -106.731207, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Saskatoon Coop", "Latitude": 52.1332, "Longitude": -106.67, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Watrous Ag Centre", "Latitude": 51.676963, "Longitude": -105.483289, "Contact": "Volodymyr Vakula", "Phone": "306-917-8778"},
    {"Location": "Abbey Farm Centre", "Latitude": 50.7369, "Longitude": -108.7575, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Cabri Agro Centre", "Latitude": 50.62, "Longitude": -108.46, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Consul Agro Centre & Food Store", "Latitude": 49.295378, "Longitude": -109.520113, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Eastend Agro Centre", "Latitude": 49.51, "Longitude": -108.82, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Frontier Agro Centre", "Latitude": 49.204894, "Longitude": -108.561809, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Gull Lake Agro Centre & Gas Bar", "Latitude": 50.1, "Longitude": -108.4, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Hazlet Agro Centre", "Latitude": 50.400062, "Longitude": -108.593949, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Herbert Farm Centre & Gas Bar", "Latitude": 50.427523, "Longitude": -107.223438, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Kyle Agro Centre", "Latitude": 50.832702, "Longitude": -108.039213, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Maple Creek Agro Centre", "Latitude": 49.8, "Longitude": -109.1, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Morse Farm Centre", "Latitude": 50.33425, "Longitude": -106.9663, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Ponteix Farm Centre", "Latitude": 49.74138, "Longitude": -107.469433, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Sceptre Farm Centre", "Latitude": 50.86281, "Longitude": -109.27075, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Shaunavon Home & Agro Centre", "Latitude": 49.644498, "Longitude": -108.415893, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Swift Current Agro", "Latitude": 50.285765, "Longitude": -107.851187, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Tompkins Farm Centre", "Latitude": 50.067518, "Longitude": -108.805208, "Contact": "Zane Banadyga", "Phone": "306-750-6603"},
    {"Location": "Arcola Agro & Farm Hardware Centre", "Latitude": 49.635031, "Longitude": -102.490666, "Contact": "", "Phone": "(306) 455-2393"},
    {"Location": "Arrowwood Agro Centre & Tire Shop", "Latitude": 50.737998, "Longitude": -113.150747, "Contact": "", "Phone": "(403) 534-3800"},
    {"Location": "Assiniboia Agro Centre", "Latitude": 49.631119, "Longitude": -105.992649, "Contact": "", "Phone": "(306) 642-4933"},
    {"Location": "Austin TransCanada Agro Centre", "Latitude": 49.948712, "Longitude": -98.936505, "Contact": "", "Phone": "204-637-3030"},
    {"Location": "Avonhurst Agro Centre", "Latitude": 50.637581, "Longitude": -104.131317, "Contact": "", "Phone": "(306) 771-2812"},
    {"Location": "Barons Fertilizer/Agro", "Latitude": 49.997493, "Longitude": -113.081528, "Contact": "", "Phone": "(403) 757-2054"},
    {"Location": "Barrhead Home & Agro Centre", "Latitude": 54.126236, "Longitude": -114.404412, "Contact": "", "Phone": "(780) 674-2201"},
    {"Location": "Beausejour Agro Centre", "Latitude": 50.061931, "Longitude": -96.514082, "Contact": "", "Phone": "(431) 218-9950"},
    {"Location": "Birtle Home & Agro Centre", "Latitude": 50.421841, "Longitude": -101.046201, "Contact": "", "Phone": "(204) 842-3388"},
    {"Location": "Boissevain Home & Agro Centre", "Latitude": 49.230602, "Longitude": -100.055805, "Contact": "", "Phone": "(204) 534-2411"},
    {"Location": "Bonnyville Agro Centre", "Latitude": 54.267925, "Longitude": -110.741515, "Contact": "", "Phone": "(780) 826-3349"},
    {"Location": "Borden Agro Liquor Centre", "Latitude": 52.405065, "Longitude": -107.23865, "Contact": "", "Phone": "(306) 997-2033"},
    {"Location": "Brandon Agro Centre", "Latitude": 49.851114, "Longitude": -99.960892, "Contact": "", "Phone": "(204) 763-8998"},
    {"Location": "Broadview Agro Centre", "Latitude": 50.378923, "Longitude": -102.583995, "Contact": "", "Phone": "(306) 696-3038"},
    {"Location": "Brooks Agro Centre", "Latitude": 50.571027, "Longitude": -111.893005, "Contact": "", "Phone": "(403) 362-2885"},
    {"Location": "Broxburn Co-op", "Latitude": 49.694578, "Longitude": -112.833103, "Contact": "", "Phone": ""},
    {"Location": "Bruno Co-op", "Latitude": 52.262184, "Longitude": -105.524021, "Contact": "", "Phone": ""},
    {"Location": "Central Butte Home & Agro Centre", "Latitude": 50.79383, "Longitude": -106.507831, "Contact": "", "Phone": "(306) 796-2115"},
    {"Location": "Churchbridge Home & Agro Centre", "Latitude": 50.900089, "Longitude": -101.893302, "Contact": "", "Phone": "(306) 896-2533"},
    {"Location": "Claresholm Fertilizer/Agro", "Latitude": 50.023946, "Longitude": -113.580028, "Contact": "", "Phone": "(403) 625-4088"},
    {"Location": "Co-op Farm & Hardware (Watrous)", "Latitude": 51.675148, "Longitude": -105.465539, "Contact": "", "Phone": "(306) 946-5511"},
    {"Location": "Cowley Home & Agro Centre", "Latitude": 49.56901, "Longitude": -114.073325, "Contact": "", "Phone": "(403) 628-3763"},
    {"Location": "Craik Home & Agro Centre", "Latitude": 51.051506, "Longitude": -105.815931, "Contact": "", "Phone": "(306) 734-2616"},
    {"Location": "Cupar Agro Centre", "Latitude": 50.947688, "Longitude": -104.213978, "Contact": "", "Phone": "(306) 723-1200"},
    {"Location": "Cypress River Agro Centre", "Latitude": 49.47232, "Longitude": -98.862716, "Contact": "", "Phone": "(204) 743-2314"},
    {"Location": "Dauphin Agro Centre", "Latitude": 51.153509, "Longitude": -100.04425, "Contact": "", "Phone": "(204) 622-6080"},
    {"Location": "Dawson Co-op Home & Agro Centre", "Latitude": 55.760531, "Longitude": -120.236445, "Contact": "", "Phone": "(250) 782-3371"},
    {"Location": "Domain Farm Supply", "Latitude": 49.6, "Longitude": -97.316667, "Contact": "", "Phone": "(204) 736-4321"},
    {"Location": "Duperow Co-op Agro Centre", "Latitude": 52.05765, "Longitude": -107.984618, "Contact": "", "Phone": "(306) 948-2706"},
    {"Location": "Ebenezer Agro Centre", "Latitude": 51.370535, "Longitude": -102.447115, "Contact": "", "Phone": "(306) 782-7434"},
    {"Location": "Elkhorn Agro Centre", "Latitude": 49.975341, "Longitude": -101.236587, "Contact": "", "Phone": "(204) 845-2438"},
    {"Location": "Foremost Agro Centre", "Latitude": 49.477047, "Longitude": -111.445316, "Contact": "", "Phone": "(403) 867-3200"},
    {"Location": "Foxwarren Fertilizer Plant", "Latitude": 50.421841, "Longitude": -101.046201, "Contact": "", "Phone": "(204) 859-0444"},
    {"Location": "Gladstone Agro Centre", "Latitude": 50.22549, "Longitude": -98.951066, "Contact": "", "Phone": "(204) 385-2906"},
    {"Location": "Grand Prairie", "Latitude": 55.16832, "Longitude": -118.79279, "Contact": "", "Phone": ""},
    {"Location": "Hafford Agro Centre", "Latitude": 52.729733, "Longitude": -107.356791, "Contact": "", "Phone": "(306) 549-2166"},
    {"Location": "Head Office", "Latitude": 51.006727, "Longitude": -114.062842, "Contact": "", "Phone": ""},
    {"Location": "Herbert Farm Centre & Gas Bar", "Latitude": 50.426316, "Longitude": -107.220189, "Contact": "", "Phone": "(306) 784-3241"},
    {"Location": "High River Fertilizer/Agro", "Latitude": 50.580192, "Longitude": -113.870933, "Contact": "", "Phone": "(403) 652-4143"},
    {"Location": "Homewood Agro Centre", "Latitude": 49.508926, "Longitude": -97.865911, "Contact": "", "Phone": "(204) 745-6421"},
    {"Location": "Horseshoe Canyon Agro Centre", "Latitude": 51.416933, "Longitude": -112.640238, "Contact": "", "Phone": "(403) 677-2777"},
    {"Location": "Hudson Bay Agro Centre", "Latitude": 52.856598, "Longitude": -102.396807, "Contact": "", "Phone": "(306) 865-2288"},
    {"Location": "Humboldt Agro Centre", "Latitude": 52.202009, "Longitude": -105.123037, "Contact": "", "Phone": "(306) 682-2252"},
    {"Location": "Ituna Home & Agro", "Latitude": 51.170571, "Longitude": -103.495756, "Contact": "", "Phone": "(306) 795-2441"},
    {"Location": "Kelliher Agro Centre and Gas Bar", "Latitude": 51.266667, "Longitude": -103.733333, "Contact": "", "Phone": "(306) 675-2156"},
    {"Location": "Kindersley Co-op Kerrobert Cardlock", "Latitude": 51.915887, "Longitude": -109.136544, "Contact": "", "Phone": "(306) 463-3812"},
    {"Location": "La Crete Co-op", "Latitude": 58.19095, "Longitude": -116.395711, "Contact": "", "Phone": ""},
    {"Location": "Lamont Agro Centre", "Latitude": 53.783451, "Longitude": -112.447814, "Contact": "", "Phone": "(780) 895-2241"},
    {"Location": "Lang Gas Bar", "Latitude": 49.918915, "Longitude": -104.371512, "Contact": "", "Phone": "(306) 464-2008"},
    {"Location": "Leduc Home & Agro Centre", "Latitude": 53.260782, "Longitude": -113.551168, "Contact": "", "Phone": "(780) 986-3000"},
    {"Location": "Leroy Agro Center", "Latitude": 52.002272, "Longitude": -104.740329, "Contact": "", "Phone": "(306) 286-3221"},
    {"Location": "Lethbridge County Agro and Crop Protecti", "Latitude": 53.284101, "Longitude": -103.584595, "Contact": "", "Phone": "(403) 394-2476"},
    {"Location": "Limerick Agro Centre/Gas Bar", "Latitude": 49.653423, "Longitude": -106.268567, "Contact": "", "Phone": "(306) 263-2033"},
    {"Location": "Lipton Farm Supply", "Latitude": 50.901424, "Longitude": -103.850734, "Contact": "", "Phone": "(306) 336-2333"},
    {"Location": "Lomond Agro Centre & Fertilizer", "Latitude": 50.350439, "Longitude": -112.641492, "Contact": "", "Phone": "(403) 792-3757"},
    {"Location": "Lowe Farm Crop Protection", "Latitude": 49.354747, "Longitude": -97.587343, "Contact": "", "Phone": "(204) 746-2684"},
    {"Location": "Lucky Lake Agro Centre", "Latitude": 50.985022, "Longitude": -107.137321, "Contact": "", "Phone": "(306) 858-2660"},
    {"Location": "MacGregor Farm & Building Centre", "Latitude": 49.966285, "Longitude": -98.778816, "Contact": "", "Phone": "(204) 685-2033"},
    {"Location": "McMahon Farm Supply", "Latitude": 50.073806, "Longitude": -107.555976, "Contact": "", "Phone": "(306) 627-3434"},
    {"Location": "Meadow Lake Co-op", "Latitude": 54.13026, "Longitude": -108.435059, "Contact": "", "Phone": "306-236-4474"},
    {"Location": "Medicine Hat Agro & Hardware Centre", "Latitude": 50.201734, "Longitude": -110.515021, "Contact": "", "Phone": "(403) 528-6609"},
    {"Location": "Melville Home & Agro Centre", "Latitude": 50.930724, "Longitude": -102.807296, "Contact": "", "Phone": "(306) 728-4461"},
    {"Location": "Miniota Agro Centre", "Latitude": 50.133333, "Longitude": -101.033333, "Contact": "", "Phone": "(204) 567-3766"},
    {"Location": "Minnedosa Agro Centre", "Latitude": 50.249865, "Longitude": -99.838523, "Contact": "", "Phone": "(204) 867-2749"},
    {"Location": "Montmartre Agro Centre", "Latitude": 50.21761, "Longitude": -103.448371, "Contact": "", "Phone": "(306) 424-2293"},
    {"Location": "Moosehorn Hardware", "Latitude": 51.28905, "Longitude": -98.422117, "Contact": "", "Phone": "(204) 768-2770"},
    {"Location": "Morden Agro Centre", "Latitude": 49.192706, "Longitude": -98.101457, "Contact": "", "Phone": "(204) 325-1658"},
    {"Location": "Neepawa Agro Centre", "Latitude": 50.228417, "Longitude": -99.466401, "Contact": "", "Phone": "(204) 476-3431"},
    {"Location": "Neerlandia Coop Home & Farm, Agro Centre", "Latitude": 54.172301, "Longitude": -114.548565, "Contact": "", "Phone": "(780) 674-3020"},
    {"Location": "Nipawin Co-op", "Latitude": 53.362788, "Longitude": -104.017425, "Contact": "", "Phone": "306-862-4595"},
    {"Location": "North Battleford Agro Centre", "Latitude": 52.776186, "Longitude": -108.300476, "Contact": "", "Phone": "(306) 446-7288"},
    {"Location": "Notre Dame Agro Centre", "Latitude": 49.529002, "Longitude": -98.55666, "Contact": "", "Phone": "(204) 248-2331"},
    {"Location": "Olds Co-op", "Latitude": 51.80032, "Longitude": -114.099052, "Contact": "", "Phone": ""},
    {"Location": "Oyen Bulk Petroleum and Agro", "Latitude": 51.34889, "Longitude": -110.487197, "Contact": "", "Phone": "(403) 664-3633"},
    {"Location": "Peebles Home & Agro Centre", "Latitude": 50.15999, "Longitude": -102.945666, "Contact": "", "Phone": "(306) 224-4521"},
    {"Location": "Pierson Agro Centre", "Latitude": 49.17821, "Longitude": -101.262358, "Contact": "", "Phone": "(204) 634-2328"},
    {"Location": "Pincher Creek Home & Agro Centre", "Latitude": 49.485667, "Longitude": -113.950292, "Contact": "", "Phone": "(403) 627-3606"},
    {"Location": "Pitt Meadows Home & Agro Centre", "Latitude": 49.220762, "Longitude": -122.690153, "Contact": "", "Phone": "(604) 465-5651"},
    {"Location": "Plamondon Home, Energy & Agro Centre", "Latitude": 54.848511, "Longitude": -112.343948, "Contact": "", "Phone": "(780) 798-3827"},
    {"Location": "Prince George Co-op Capital Feeds", "Latitude": 53.912864, "Longitude": -122.74537, "Contact": "", "Phone": "(250) 564-6010"},
    {"Location": "Quesnel Agro Centre & Convenience Store", "Latitude": 52.979428, "Longitude": -122.493627, "Contact": "", "Phone": "(250) 992-7274"},
    {"Location": "Radisson Home & Agro Centre", "Latitude": 52.461024, "Longitude": -107.395049, "Contact": "", "Phone": "(306) 827-2206"},
    {"Location": "Rama Agro Main Store", "Latitude": 51.760151, "Longitude": -102.997915, "Contact": "", "Phone": "(306) 593-6006"},
    {"Location": "Raymore Home & Agro Centre", "Latitude": 51.409625, "Longitude": -104.529238, "Contact": "", "Phone": "(306) 746-5861"},
    {"Location": "Redvers Co-op", "Latitude": 49.572757, "Longitude": -101.69799, "Contact": "", "Phone": ""},
    {"Location": "Roblin Agro Centre", "Latitude": 51.228216, "Longitude": -101.355058, "Contact": "", "Phone": "(204) 937-6402"},
    {"Location": "Rockglen Agro Centre", "Latitude": 49.179398, "Longitude": -105.94724, "Contact": "", "Phone": "(306) 476-2210"},
    {"Location": "Rossburn Agro Centre", "Latitude": 50.668359, "Longitude": -100.810698, "Contact": "", "Phone": "(204) 859-3203"},
    {"Location": "Russell Agro Centre", "Latitude": 50.780454, "Longitude": -101.287879, "Contact": "", "Phone": "(204) 773-2166"},
    {"Location": "Sedalia Agro Centre and Food Store", "Latitude": 51.675493, "Longitude": -110.665261, "Contact": "", "Phone": "(403) 326-2152"},
    {"Location": "Shamrock Agro Centre and Cardlock", "Latitude": 50.195306, "Longitude": -106.637098, "Contact": "", "Phone": "(306) 648-3597"},
    {"Location": "Shellbrook Agro Centre", "Latitude": 53.22248, "Longitude": -106.387482, "Contact": "", "Phone": "(306) 747-2122"},
    {"Location": "Saint Joseph Agro Centre", "Latitude": 49.13375, "Longitude": -97.39143, "Contact": "", "Phone": "(204) 737-2111"},
    {"Location": "St. Paul Home & Agro Centre", "Latitude": 53.987647, "Longitude": -111.291114, "Contact": "", "Phone": "(780) 645-1746"},
    {"Location": "Ste. Rose du Lac Agro Centre", "Latitude": 51.059125, "Longitude": -99.519423, "Contact": "", "Phone": "(204) 447-4270"},
    {"Location": "Steinbach Agro Centre", "Latitude": 49.525441, "Longitude": -96.685428, "Contact": "", "Phone": "(204) 326-9921"},
    {"Location": "Stewart Valley Farm Centre", "Latitude": 50.596911, "Longitude": -107.806822, "Contact": "", "Phone": "(306) 778-5336"},
    {"Location": "Strathclair Agro Centre", "Latitude": 50.404463, "Longitude": -100.395537, "Contact": "", "Phone": "(204) 365-2491"},
    {"Location": "Swan River Agro Centre", "Latitude": 52.06581, "Longitude": -101.29976, "Contact": "", "Phone": "(204) 836-2109"},
    {"Location": "The Pas Agro Centre", "Latitude": 53.822417, "Longitude": -101.240515, "Contact": "", "Phone": "(204) 623-6934"},
    {"Location": "Tugaske Home & Agro Centre", "Latitude": 50.874074, "Longitude": -106.286491, "Contact": "", "Phone": "(306) 759-2222"},
    {"Location": "Turtleford Farm Supply & Home Centre", "Latitude": 53.387847, "Longitude": -108.958192, "Contact": "", "Phone": "(306) 845-2162"},
    {"Location": "Vanderhoof Agro Centre", "Latitude": 54.017529, "Longitude": -124.007663, "Contact": "", "Phone": "(250) 567-4225"},
    {"Location": "Vauxhall Home & Agro Centre & Tire Shop", "Latitude": 50.069185, "Longitude": -112.107691, "Contact": "", "Phone": "(403) 654-2137"},
    {"Location": "Veregin Agro Centre", "Latitude": 51.581741, "Longitude": -102.076742, "Contact": "", "Phone": "(306) 542-4378"},
    {"Location": "Virden Agro Centre", "Latitude": 49.848509, "Longitude": -100.932265, "Contact": "", "Phone": "(204) 748-2843"},
    {"Location": "Wainwright Home & Agro Centre", "Latitude": 52.840272, "Longitude": -110.851434, "Contact": "", "Phone": "(780) 842-4181"},
    {"Location": "Wetaskiwin Agro Centre", "Latitude": 52.968492, "Longitude": -113.36792, "Contact": "", "Phone": "(780) 352-3359"},
    {"Location": "Woodrow Agro Centre/Gas Bar", "Latitude": 49.694814, "Longitude": -106.725313, "Contact": "", "Phone": "(306) 472-3742"},
    {"Location": "Yorkton Agro Centre and Cardlock", "Latitude": 51.212045, "Longitude": -102.461243, "Contact": "", "Phone": "(306) 782-2451"},
    {"Location": "Young Gas Bar and Agro", "Latitude": 51.769073, "Longitude": -105.748323, "Contact": "", "Phone": "(306) 259-2131"},
    {"Location": "Evergreen Coop", "Latitude": 52.37688, "Longitude": -114.9184, "Contact": "", "Phone": ""},
    {"Location": "GRASSROOTS CO-OPERATIVE LIMITED", "Latitude": 49.687757, "Longitude": -107.138789, "Contact": "", "Phone": "306-264-5111"},
    {"Location": "TURTLEFORD CO-OP ASSN LTD.", "Latitude": 53.086227, "Longitude": -109.296096, "Contact": "", "Phone": "306-893-1222"}
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
