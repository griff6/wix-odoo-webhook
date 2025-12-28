#!/usr/bin/env python3
"""
export_leads_json.py

Exports Odoo CRM leads (crm.lead) to a JSON file in a stable, “nearby search”-friendly format.

Assumes you already have:
- odoo_connector.py in the same folder (or on PYTHONPATH)
- credentials configured inside odoo_connector.py (ODOO_URL/ODOO_DB/ODOO_USERNAME/ODOO_PASSWORD)
"""

from __future__ import annotations
from datetime import timezone

import argparse
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from odoo_connector import connect_odoo, normalize_state  # uses your existing code


def _iso(v: Any) -> Optional[str]:
    """Convert Odoo datetime/date objects (or strings) to ISO strings safely."""
    if v is None or v is False or v == "":
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    # Sometimes Odoo returns date/datetime as strings already
    return str(v)


def _m2o_name(v: Any) -> Optional[str]:
    """
    Many2one values from search_read are usually:
    - False
    - [id, display_name]
    """
    if not v:
        return None
    if isinstance(v, (list, tuple)) and len(v) >= 2:
        return str(v[1])
    return str(v)


def _m2o_id(v: Any) -> Optional[int]:
    if not v:
        return None
    if isinstance(v, (list, tuple)) and len(v) >= 1:
        try:
            return int(v[0])
        except Exception:
            return None
    try:
        return int(v)
    except Exception:
        return None


def fetch_all_crm_leads(
    include_active_only: bool = True,
    types: Tuple[str, ...] = ("lead", "opportunity"),
    page_size: int = 500,
) -> List[Dict[str, Any]]:
    """
    Fetches crm.lead records using paged search_read.

    types:
      - "lead" and/or "opportunity" (Odoo uses crm.lead.type)
    """
    uid, models = connect_odoo()
    if not uid or not models:
        raise RuntimeError("Could not authenticate to Odoo (connect_odoo failed).")

    # Domain
    domain: List[Any] = []
    if include_active_only:
        domain.append(("active", "=", True))

    # Filter by type if provided
    if types:
        if len(types) == 1:
            domain.append(("type", "=", types[0]))
        else:
            domain.append(("type", "in", list(types)))

    # Fields to export (tuned for location + CRM triage)
    fields = [
        "id",
        "name",
        "type",
        "active",
        "contact_name",
        "partner_name",
        "email_from",
        "phone",
        "mobile",
        "street",
        "street2",
        "city",
        "state_id",
        "zip",
        "country_id",
        "stage_id",
        "team_id",
        "user_id",
        "source_id",
        "tag_ids",
        "description",
        "expected_revenue",
        "probability",
        "create_date",
        "write_date",
    ]

    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        rows = models.execute_kw(
            # NOTE: odoo_connector.py holds ODOO_DB + ODOO_PASSWORD; connect_odoo() uses them already
            # so we just re-use the same module-level constants implicitly via the server session.
            # execute_kw signature requires db + uid + password. We'll import them from odoo_connector
            # if you prefer, but keeping this minimal: pull them from the odoo_connector module.
            __import__("odoo_connector").ODOO_DB,
            uid,
            __import__("odoo_connector").ODOO_PASSWORD,
            "crm.lead",
            "search_read",
            [domain],
            {"fields": fields, "limit": page_size, "offset": offset, "order": "id"},
        )

        if not rows:
            break

        all_rows.extend(rows)
        offset += len(rows)

    return all_rows


def transform(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Transform raw Odoo rows into a stable schema for your radius search tooling.
    """
    out: List[Dict[str, Any]] = []

    for r in rows:
        state_name = _m2o_name(r.get("state_id"))
        province_code = normalize_state(state_name) if state_name else None

        out.append(
            {
                "lead_id": r.get("id"),
                "name": r.get("name"),
                "type": r.get("type"),
                "active": bool(r.get("active")),

                # Contact-ish fields
                "contact_name": r.get("contact_name") or None,
                "company_name": r.get("partner_name") or None,
                "email": r.get("email_from") or None,
                "phone": r.get("phone") or None,
                "mobile": r.get("mobile") or None,

                # Address fields (city/province is your “good enough” locator)
                "street": r.get("street") or None,
                "street2": r.get("street2") or None,
                "city": r.get("city") or None,
                "province_state_name": state_name,
                "province_state_code": province_code,
                "postal_code": r.get("zip") or None,
                "country_name": _m2o_name(r.get("country_id")),
                "country_id": _m2o_id(r.get("country_id")),

                # CRM fields
                "stage_name": _m2o_name(r.get("stage_id")),
                "stage_id": _m2o_id(r.get("stage_id")),
                "salesperson": _m2o_name(r.get("user_id")),
                "salesperson_id": _m2o_id(r.get("user_id")),
                "team": _m2o_name(r.get("team_id")),
                "team_id": _m2o_id(r.get("team_id")),
                "source": _m2o_name(r.get("source_id")),
                "source_id": _m2o_id(r.get("source_id")),
                "tag_ids": r.get("tag_ids") or [],

                "expected_revenue": r.get("expected_revenue"),
                "probability": r.get("probability"),

                "created_at": _iso(r.get("create_date")),
                "updated_at": _iso(r.get("write_date")),

                # Optional—sometimes useful for debugging / context:
                "description": r.get("description") or None,
            }
        )

    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(out),
        "schema_version": 1,
        "records": out,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Export Odoo CRM leads to JSON for geo-radius searching.")
    ap.add_argument("--out", default="leads_export.json", help="Output JSON path (default: leads_export.json)")
    ap.add_argument("--include-inactive", action="store_true", help="Include inactive/archived leads")
    ap.add_argument(
        "--types",
        default="lead,opportunity",
        help="Comma-separated list of crm.lead types to export: lead,opportunity (default: both)",
    )
    ap.add_argument("--page-size", type=int, default=500, help="Odoo page size for search_read (default: 500)")
    args = ap.parse_args()

    types = tuple(t.strip() for t in args.types.split(",") if t.strip())
    include_active_only = not args.include_inactive

    rows = fetch_all_crm_leads(
        include_active_only=include_active_only,
        types=types,  # type: ignore[arg-type]
        page_size=args.page_size,
    )
    payload = transform(rows)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"Exported {payload['record_count']} CRM lead(s) to: {args.out}")


if __name__ == "__main__":
    main()
