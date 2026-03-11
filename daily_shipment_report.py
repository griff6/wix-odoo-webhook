#!/usr/bin/env python3
"""Daily Odoo shipment report emailer.

This script is intended to run once per day (for example via cron at 5:00 PM).
It fetches completed outbound pickings since the last successful run, groups data by
sales order, and emails a shipment/backorder summary.
"""

from __future__ import annotations

import json
import os
import smtplib
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from odoo_connector import ODOO_DB, ODOO_PASSWORD, ODOO_URL, connect_odoo


DEFAULT_TZ = "America/Regina"
DEFAULT_LOOKBACK_HOURS = 24


@dataclass
class ReportConfig:
    recipients: list[str]
    sender: str
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_use_tls: bool
    resend_api_key: str
    timezone_name: str
    state_file: Path
    first_run_lookback_hours: int
    range_start_utc: str
    range_end_utc: str
    send_empty_report: bool
    dry_run: bool


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def load_config() -> ReportConfig:
    recipients_raw = os.getenv("SHIPMENT_REPORT_RECIPIENTS", "")
    recipients = [item.strip() for item in recipients_raw.split(",") if item.strip()]

    smtp_port_raw = (os.getenv("SMTP_PORT") or "587").strip()
    try:
        smtp_port = int(smtp_port_raw)
    except ValueError as exc:
        raise ValueError(f"Invalid SMTP_PORT: {smtp_port_raw}") from exc

    lookback_raw = (os.getenv("SHIPMENT_REPORT_FIRST_RUN_LOOKBACK_HOURS") or str(DEFAULT_LOOKBACK_HOURS)).strip()
    try:
        lookback_hours = int(lookback_raw)
    except ValueError as exc:
        raise ValueError(f"Invalid SHIPMENT_REPORT_FIRST_RUN_LOOKBACK_HOURS: {lookback_raw}") from exc

    return ReportConfig(
        recipients=recipients,
        sender=(os.getenv("SHIPMENT_REPORT_SENDER") or "").strip(),
        smtp_host=(os.getenv("SMTP_HOST") or "").strip(),
        smtp_port=smtp_port,
        smtp_username=(os.getenv("SMTP_USERNAME") or "").strip(),
        smtp_password=(os.getenv("SMTP_PASSWORD") or "").strip(),
        smtp_use_tls=_env_bool("SMTP_USE_TLS", True),
        resend_api_key=(os.getenv("RESEND_API_KEY") or "").strip(),
        timezone_name=(os.getenv("SHIPMENT_REPORT_TIMEZONE") or DEFAULT_TZ).strip(),
        state_file=Path((os.getenv("SHIPMENT_REPORT_STATE_FILE") or ".shipment_report_state.json").strip()),
        first_run_lookback_hours=lookback_hours,
        range_start_utc=(os.getenv("SHIPMENT_REPORT_RANGE_START_UTC") or "").strip(),
        range_end_utc=(os.getenv("SHIPMENT_REPORT_RANGE_END_UTC") or "").strip(),
        send_empty_report=_env_bool("SHIPMENT_REPORT_SEND_EMPTY", False),
        dry_run=_env_bool("SHIPMENT_REPORT_DRY_RUN", False),
    )


def validate_config(config: ReportConfig) -> None:
    missing = []
    if not config.recipients:
        missing.append("SHIPMENT_REPORT_RECIPIENTS")
    if not config.sender:
        missing.append("SHIPMENT_REPORT_SENDER")
    using_resend = bool(config.resend_api_key)
    if not using_resend:
        if not config.smtp_host:
            missing.append("SMTP_HOST")
        if not config.smtp_username:
            missing.append("SMTP_USERNAME")
        if not config.smtp_password:
            missing.append("SMTP_PASSWORD")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    try:
        ZoneInfo(config.timezone_name)
    except Exception as exc:
        raise ValueError(f"Invalid SHIPMENT_REPORT_TIMEZONE: {config.timezone_name}") from exc

    if bool(config.range_start_utc) != bool(config.range_end_utc):
        raise ValueError(
            "Set both SHIPMENT_REPORT_RANGE_START_UTC and SHIPMENT_REPORT_RANGE_END_UTC, or neither."
        )


def load_last_run(state_file: Path) -> datetime | None:
    if not state_file.exists():
        return None
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8"))
        raw = payload.get("last_run_utc")
        if not raw:
            return None
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def save_last_run(state_file: Path, dt_utc: datetime) -> None:
    payload = {"last_run_utc": dt_utc.astimezone(UTC).isoformat()}
    state_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _dt_to_odoo(dt_utc: datetime) -> str:
    return dt_utc.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _parse_utc_env(value: str, field_name: str) -> datetime:
    v = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(v, fmt)
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(
        f"Invalid {field_name}: '{value}'. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' in UTC."
    )


def _field_id(value: Any) -> int | None:
    if isinstance(value, (list, tuple)) and value:
        try:
            return int(value[0])
        except (TypeError, ValueError):
            return None
    if isinstance(value, int):
        return value
    return None


def _field_name(value: Any) -> str:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return str(value[1])
    return ""


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def fetch_done_pickings(models: Any, uid: int, start_utc: datetime, end_utc: datetime) -> list[dict[str, Any]]:
    domain = [
        ("state", "=", "done"),
        ("picking_type_code", "=", "outgoing"),
        ("date_done", ">=", _dt_to_odoo(start_utc)),
        ("date_done", "<", _dt_to_odoo(end_utc)),
    ]
    fields = ["id", "name", "date_done", "origin", "sale_id"]
    return models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        "stock.picking",
        "search_read",
        [domain],
        {"fields": fields, "order": "date_done asc"},
    )


def fetch_sale_ids_for_pickings(models: Any, uid: int, pickings: list[dict[str, Any]]) -> dict[int, int]:
    result: dict[int, int] = {}
    unresolved_by_origin: dict[str, list[int]] = defaultdict(list)

    for p in pickings:
        picking_id = int(p["id"])
        sale_id = _field_id(p.get("sale_id"))
        if sale_id:
            result[picking_id] = sale_id
            continue
        origin = (p.get("origin") or "").strip()
        if origin:
            unresolved_by_origin[origin].append(picking_id)

    if not unresolved_by_origin:
        return result

    origin_names = list(unresolved_by_origin.keys())
    sale_orders = models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        "sale.order",
        "search_read",
        [[("name", "in", origin_names)]],
        {"fields": ["id", "name"]},
    )
    sale_by_name = {so.get("name"): int(so["id"]) for so in sale_orders if so.get("name")}

    for origin, picking_ids in unresolved_by_origin.items():
        sale_id = sale_by_name.get(origin)
        if not sale_id:
            continue
        for picking_id in picking_ids:
            result[picking_id] = sale_id

    return result


def fetch_shipped_lines(models: Any, uid: int, picking_ids: list[int]) -> dict[int, dict[str, float]]:
    if not picking_ids:
        return {}

    moves = models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        "stock.move",
        "search_read",
        [[("picking_id", "in", picking_ids), ("quantity_done", ">", 0)]],
        {"fields": ["picking_id", "product_id", "quantity_done"]},
    )

    by_picking: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for move in moves:
        picking_id = _field_id(move.get("picking_id"))
        if not picking_id:
            continue
        product_name = _field_name(move.get("product_id")) or "Unknown Product"
        by_picking[picking_id][product_name] += _safe_float(move.get("quantity_done"))

    return {pid: dict(products) for pid, products in by_picking.items()}


def fetch_backorders_by_sale(models: Any, uid: int, sale_ids: set[int]) -> dict[int, dict[str, float]]:
    if not sale_ids:
        return {}

    lines = models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        "sale.order.line",
        "search_read",
        [[("order_id", "in", list(sale_ids)), ("display_type", "=", False)]],
        {"fields": ["order_id", "product_id", "product_uom_qty", "qty_delivered"]},
    )

    backorders: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for line in lines:
        sale_id = _field_id(line.get("order_id"))
        if not sale_id:
            continue
        ordered = _safe_float(line.get("product_uom_qty"))
        delivered = _safe_float(line.get("qty_delivered"))
        remaining = ordered - delivered
        if remaining <= 1e-6:
            continue
        product_name = _field_name(line.get("product_id")) or "Unknown Product"
        backorders[sale_id][product_name] += remaining

    return {sale_id: dict(products) for sale_id, products in backorders.items()}


def fetch_sale_order_details(models: Any, uid: int, sale_ids: set[int]) -> dict[int, dict[str, Any]]:
    if not sale_ids:
        return {}

    sales = models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        "sale.order",
        "search_read",
        [[("id", "in", list(sale_ids))]],
        {"fields": ["id", "name", "partner_id"]},
    )

    result: dict[int, dict[str, Any]] = {}
    for sale in sales:
        sid = int(sale["id"])
        result[sid] = {
            "name": sale.get("name") or f"Sale {sid}",
            "customer": _field_name(sale.get("partner_id")) or "Unknown Customer",
        }
    return result


def format_report(
    pickings: list[dict[str, Any]],
    picking_to_sale: dict[int, int],
    shipped_by_picking: dict[int, dict[str, float]],
    backorders_by_sale: dict[int, dict[str, float]],
    sale_details: dict[int, dict[str, Any]],
    start_utc: datetime,
    end_utc: datetime,
    tz_name: str,
) -> tuple[str, str]:
    tz = ZoneInfo(tz_name)
    local_start = start_utc.astimezone(tz)
    local_end = end_utc.astimezone(tz)

    by_sale: dict[int, dict[str, Any]] = {}
    no_sale_entries: list[dict[str, Any]] = []

    for picking in pickings:
        picking_id = int(picking["id"])
        sale_id = picking_to_sale.get(picking_id)
        shipped_products = shipped_by_picking.get(picking_id, {})
        if not shipped_products:
            continue

        line_entry = {
            "picking_name": picking.get("name") or f"PICK-{picking_id}",
            "date_done": picking.get("date_done") or "",
            "products": shipped_products,
        }

        if not sale_id:
            no_sale_entries.append(line_entry)
            continue

        if sale_id not in by_sale:
            details = sale_details.get(sale_id, {})
            by_sale[sale_id] = {
                "sale_name": details.get("name") or f"Sale {sale_id}",
                "customer": details.get("customer") or "Unknown Customer",
                "pickings": [],
                "shipped_totals": defaultdict(float),
            }

        sale_entry = by_sale[sale_id]
        sale_entry["pickings"].append(line_entry)
        for product_name, qty in shipped_products.items():
            sale_entry["shipped_totals"][product_name] += qty

    report_date = local_end.strftime("%Y-%m-%d")
    subject = f"[Wavcor] Daily Shipment Report - {report_date}"

    lines = []
    lines.append("Daily Odoo Shipment Report")
    lines.append(
        f"Window: {local_start.strftime('%Y-%m-%d %H:%M %Z')} to {local_end.strftime('%Y-%m-%d %H:%M %Z')}"
    )
    lines.append("")

    if not by_sale and not no_sale_entries:
        lines.append("No shipped products were found in this window.")
        return subject, "\n".join(lines)

    for sale_id in sorted(by_sale, key=lambda sid: by_sale[sid]["sale_name"]):
        sale_entry = by_sale[sale_id]
        sale_name = sale_entry["sale_name"]
        customer = sale_entry["customer"]
        sale_url = f"{ODOO_URL}/web#id={sale_id}&model=sale.order&view_type=form"

        lines.append(f"Order: {sale_name} ({customer})")
        lines.append("Shipped:")
        for product_name in sorted(sale_entry["shipped_totals"]):
            qty = sale_entry["shipped_totals"][product_name]
            lines.append(f"- {product_name}: {qty:g}")

        backordered = backorders_by_sale.get(sale_id, {})
        lines.append("Backordered:")
        if backordered:
            for product_name in sorted(backordered):
                remaining = backordered[product_name]
                lines.append(f"- {product_name}: {remaining:g} remaining")
        else:
            lines.append("- None")

        lines.append(f"Sales Order Link: {sale_url}")
        lines.append("")

    if no_sale_entries:
        lines.append("Shipped Pickings Without Linked Sales Order:")
        for entry in no_sale_entries:
            lines.append(f"- {entry['picking_name']} ({entry['date_done']})")
            for product_name in sorted(entry["products"]):
                qty = entry["products"][product_name]
                lines.append(f"  {product_name}: {qty:g}")
        lines.append("")

    return subject, "\n".join(lines).rstrip()


def send_email(config: ReportConfig, subject: str, body: str) -> None:
    if config.resend_api_key:
        _send_email_resend(config, subject, body)
        return
    _send_email_smtp(config, subject, body)


def _send_email_smtp(config: ReportConfig, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.sender
    msg["To"] = ", ".join(config.recipients)
    msg.set_content(body)

    with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as server:
        if config.smtp_use_tls:
            server.starttls()
        server.login(config.smtp_username, config.smtp_password)
        server.send_message(msg)


def _send_email_resend(config: ReportConfig, subject: str, body: str) -> None:
    payload = {
        "from": config.sender,
        "to": config.recipients,
        "subject": subject,
        "text": body,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=data,
        headers={
            "Authorization": f"Bearer {config.resend_api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status >= 300:
                raise RuntimeError(f"Resend API error status={resp.status}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Resend API HTTP {exc.code}: {detail}") from exc


def main() -> int:
    try:
        config = load_config()
        validate_config(config)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    now_utc = datetime.now(UTC)
    if config.range_start_utc and config.range_end_utc:
        start_utc = _parse_utc_env(config.range_start_utc, "SHIPMENT_REPORT_RANGE_START_UTC")
        end_utc = _parse_utc_env(config.range_end_utc, "SHIPMENT_REPORT_RANGE_END_UTC")
        if end_utc <= start_utc:
            print("Configuration error: SHIPMENT_REPORT_RANGE_END_UTC must be greater than start.", file=sys.stderr)
            return 2
    else:
        last_run_utc = load_last_run(config.state_file)
        if last_run_utc is None:
            start_utc = now_utc - timedelta(hours=config.first_run_lookback_hours)
        else:
            start_utc = last_run_utc
        end_utc = now_utc

    uid, models = connect_odoo()
    if not uid or models is None:
        print("Failed to connect to Odoo.", file=sys.stderr)
        return 1

    pickings = fetch_done_pickings(models, uid, start_utc, end_utc)
    picking_ids = [int(p["id"]) for p in pickings]

    picking_to_sale = fetch_sale_ids_for_pickings(models, uid, pickings)
    shipped_by_picking = fetch_shipped_lines(models, uid, picking_ids)
    sale_ids = set(picking_to_sale.values())

    backorders_by_sale = fetch_backorders_by_sale(models, uid, sale_ids)
    sale_details = fetch_sale_order_details(models, uid, sale_ids)

    subject, body = format_report(
        pickings=pickings,
        picking_to_sale=picking_to_sale,
        shipped_by_picking=shipped_by_picking,
        backorders_by_sale=backorders_by_sale,
        sale_details=sale_details,
        start_utc=start_utc,
        end_utc=end_utc,
        tz_name=config.timezone_name,
    )

    has_shipments = "No shipped products were found in this window." not in body
    if not has_shipments and not config.send_empty_report:
        print("No shipments found. Email suppressed.")
        save_last_run(config.state_file, end_utc)
        return 0

    if config.dry_run:
        print(subject)
        print()
        print(body)
        return 0

    try:
        send_email(config, subject, body)
    except Exception as exc:
        print(f"Failed to send report email: {exc}", file=sys.stderr)
        return 1

    save_last_run(config.state_file, end_utc)
    print(f"Shipment report sent to {', '.join(config.recipients)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
