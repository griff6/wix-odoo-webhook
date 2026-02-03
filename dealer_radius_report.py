#!/usr/bin/env python3
"""
Generate a dealer-radius lead report (e.g., all leads within 50 km of EACH dealer).

Inputs:
- leads_export.json produced by your existing export_leads_json.py (or GUI "Update Leads" button)
- Dealer list comes from odoo_connector.DEALER_LOCATIONS (same as your existing tools)

Outputs:
- Excel workbook:
    - "Summary" sheet: lead counts per dealer
    - "Matches" sheet: one row per (dealer, lead) match with distance_km
Notes:
- A lead can appear under multiple dealers if it is within radius of multiple dealers.
- Uses the same city/prov geocoding + local geo_cache.json as nearby_leads_gui.py.


To run this use python3 dealer_radius_report.py --leads leads_export.json --radius-km 50 --out dealer_leads_50km.xlsx

"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

from openpyxl import Workbook

# Reuse your existing logic + cache/geocoder behavior
from nearby_leads_gui import (
    DEALER_LOCATIONS,
    load_leads_export,
    leads_within_radius,
)

DEFAULT_RADIUS_KM = 50.0

# Pick a stable set of lead fields to include in the report.
# (Anything missing will just be blank.)
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
    "source_name",
    "medium_name",
    "campaign_name",
]


def _dealer_display_name(dealer: Dict[str, Any]) -> str:
    return str(dealer.get("Location", "")).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leads", required=True, help="Path to leads_export.json")
    ap.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM, help="Radius in km (default: 50)")
    ap.add_argument("--out", default="dealer_leads_within_radius.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    print(f"Processing dealer-radius report with args: {args}")

    leads_path = Path(args.leads).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    leads = load_leads_export(leads_path)
    if not isinstance(leads, list):
        raise SystemExit("Unexpected leads export format (expected list of dicts).")
    
    print(f"Loaded {len(leads)} leads.")

    # Build workbook
    wb = Workbook()
    ws_summary = wb.active
    ws_summary.title = "Summary"

    ws_matches = wb.create_sheet("Matches")

    # Headers
    ws_summary.append(["Dealer", f"Leads within {args.radius_km:.1f} km"])
    ws_matches.append(["Dealer", "distance_km", *LEAD_COLUMNS])

    print(f"Starting processing of {len(DEALER_LOCATIONS)} dealers.")

    # Iterate all dealers
    for i, dealer in enumerate(DEALER_LOCATIONS, start=1):
        print(f"Processing dealer {i}/{len(DEALER_LOCATIONS)}: {dealer.get('Location', 'Unknown')}")
        dealer_name = _dealer_display_name(dealer)
        if not dealer_name:
            continue

        matches = leads_within_radius(dealer, leads, radius_km=float(args.radius_km))
        ws_summary.append([dealer_name, len(matches)])

        for lead in matches:
            print(f"Processing lead {lead.get('id', 'Unknown')}")
            row = [dealer_name, lead.get("distance_km", "")]
            for col in LEAD_COLUMNS:
                val = lead.get(col, "")
                # Keep Excel-friendly strings
                if isinstance(val, (dict, list)):
                    val = str(val)
                row.append(val)
            ws_matches.append(row)

        # Light progress for long runs (prints every 10 dealers)
        if i % 10 == 0:
            print(f"Processed {i}/{len(DEALER_LOCATIONS)} dealers...")

    # Basic formatting: freeze panes
    ws_summary.freeze_panes = "A2"
    ws_matches.freeze_panes = "A2"

    # Save
    wb.save(out_path)
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
