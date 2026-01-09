#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import threading
import time
import os
import sys
import subprocess

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from odoo_connector import DEALER_LOCATIONS, haversine_distance


# -----------------------------
# Files / cache
# -----------------------------
LEADS_PATH_DEFAULT = Path("leads_export.json")
GEO_CACHE_PATH = Path("geo_city_cache.json")

_geolocator = Nominatim(user_agent="WavcorLeadRadiusDesktop/1.0")


def _geo_key(city: str, prov: str, country: str = "Canada") -> str:
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

    # If we removed anything, rewrite the cache so it stays clean
    if clean and clean != raw:
        _save_geo_cache(clean)

    return clean


def _save_geo_cache(cache: Dict[str, Tuple[float, float]]) -> None:
    GEO_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


def load_leads_export(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unrecognized leads JSON format: {path}")


def geocode_city_prov(
    city: str,
    prov: str,
    cache: Dict[str, Tuple[float, float]],
    country: str = "Canada",
    attempts: int = 3,
) -> Tuple[Optional[Tuple[float, float]], bool]:
    """
    Returns: (coords, cache_hit)
      coords is (lat, lon) or None
    """
    if not city or not prov:
        return None, False

    key = _geo_key(city, prov, country)
    if key in cache:
        return cache[key], True

    query = f"{city}, {prov}, {country}"
    for i in range(1, attempts + 1):
        try:
            loc = _geolocator.geocode(query, timeout=10)
            if loc:
                cache[key] = (loc.latitude, loc.longitude)
                return cache[key], False
            return None, False
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(1.5 * i)
        except Exception:
            return None, False

    return None, False


def find_dealer(location: str) -> Dict[str, Any]:
    loc = location.strip().lower()
    for d in DEALER_LOCATIONS:
        if str(d.get("Location", "")).strip().lower() == loc:
            return d
    raise KeyError(f"Dealer not found: {location}")


def leads_within_radius(
    dealer: Dict[str, Any],
    leads: List[Dict[str, Any]],
    radius_km: float,
    default_country: str = "Canada",
    progress_cb=None,
) -> List[Dict[str, Any]]:
    dealer_lat = float(dealer["Latitude"])
    dealer_lon = float(dealer["Longitude"])

    cache = _load_geo_cache()
    cache_dirty = False

    results: List[Dict[str, Any]] = []
    skipped_missing = 0
    skipped_geocode = 0
    geocode_calls = 0

    total = len(leads)

    for idx, lead in enumerate(leads, start=1):
        city = (lead.get("city") or "").strip()
        prov = (
            (lead.get("province_state_code") or "")
            or (lead.get("province_state_name") or "")
            or (lead.get("province") or "")
        ).strip()
        country = (lead.get("country_name") or lead.get("country") or default_country).strip() or default_country

        if not city or not prov:
            skipped_missing += 1
            continue

        # fast cache lookup first
        key = _geo_key(city, prov, country)
        coords = cache.get(key)
        if coords is None:
            coords, cache_hit = geocode_city_prov(city, prov, cache, country=country)
            if coords is None:
                skipped_geocode += 1
                continue
            cache_dirty = True
            geocode_calls += 1
            # polite throttling for Nominatim
            time.sleep(1.05)

        lat, lon = coords
        dist = haversine_distance(dealer_lat, dealer_lon, lat, lon)
        if dist <= radius_km:
            out = dict(lead)
            out["distance_km"] = round(dist, 1)
            results.append(out)

        if progress_cb and idx % 25 == 0:
            progress_cb(idx, total, geocode_calls)

    if cache_dirty:
        _save_geo_cache(cache)

    results.sort(key=lambda x: x.get("distance_km", 1e9))

    if progress_cb:
        progress_cb(total, total, geocode_calls, done=True, skipped_missing=skipped_missing, skipped_geocode=skipped_geocode)

    return results


# -----------------------------
# GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Leads Near Co-op")
        self.geometry("1100x650")

        self.leads_path = tk.StringVar(value=str(LEADS_PATH_DEFAULT))
        self.radius_var = tk.StringVar(value="100")
        self.dealer_var = tk.StringVar(value="")

        self.status_var = tk.StringVar(value="Ready.")
        self.progress_var = tk.DoubleVar(value=0.0)

        self.matches: List[Dict[str, Any]] = []
        self._build_ui()

        dealers = sorted([d.get("Location", "") for d in DEALER_LOCATIONS if d.get("Location")])
        if dealers:
            self.dealer_var.set(dealers[0])

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        dealers = sorted([d.get("Location", "") for d in DEALER_LOCATIONS if d.get("Location")])
        default = dealers[0] if dealers else ""
        self.dealer_var.set(default)

        ttk.Label(top, text="Co-op Location:").grid(row=0, column=0, sticky="w")

        # Show current selection in a read-only entry
        self.dealer_display = ttk.Entry(top, textvariable=self.dealer_var, width=48, state="readonly")
        self.dealer_display.grid(row=0, column=1, padx=6, sticky="w")

        # Button to open picker
        ttk.Button(top, text="Select...", command=self._open_dealer_picker).grid(row=0, column=2, sticky="w")

        # Radius controls moved to columns 3 and 4
        ttk.Label(top, text="Radius (km):").grid(row=0, column=3, sticky="w", padx=(12, 0))
        ttk.Entry(top, textvariable=self.radius_var, width=10).grid(row=0, column=4, padx=6, sticky="w")


        ttk.Label(top, text="Leads JSON:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(top, textvariable=self.leads_path, width=60).grid(row=1, column=1, padx=6, sticky="w", pady=(8, 0))
        ttk.Button(top, text="Browse...", command=self._browse_leads).grid(row=1, column=2, sticky="w", pady=(8, 0))

        ttk.Button(top, text="Search", command=self._start_search).grid(row=0, column=5, padx=12, sticky="w")
        ttk.Button(top, text="Export CSV", command=self._export_csv).grid(row=1, column=4, padx=12, sticky="w", pady=(8, 0))

        prog = ttk.Progressbar(self, variable=self.progress_var, maximum=100)
        prog.pack(fill="x", padx=10, pady=(0, 8))

        ttk.Label(self, textvariable=self.status_var).pack(anchor="w", padx=10)

        # Results table
        cols = ("distance_km", "name", "city", "province", "stage", "phone", "email")
        self.tree = ttk.Treeview(self, columns=cols, show="headings")
        for c in cols:
            self.tree.heading(c, text=c)
        self.tree.column("distance_km", width=90, anchor="e")
        self.tree.column("name", width=360)
        self.tree.column("city", width=160)
        self.tree.column("province", width=90)
        self.tree.column("stage", width=150)
        self.tree.column("phone", width=170)
        self.tree.column("email", width=240)

        yscroll = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        self.tree.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=10)
        yscroll.pack(side="right", fill="y", padx=(0, 10), pady=10)

    def _browse_leads(self):
        path = filedialog.askopenfilename(title="Select leads_export.json", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if path:
            self.leads_path.set(path)

    def _set_status(self, msg: str):
        self.status_var.set(msg)


    def _progress(self, current: int, total: int, geocode_calls: int, done: bool = False, skipped_missing: int = 0, skipped_geocode: int = 0):
        pct = (current / total) * 100 if total else 0
        self.progress_var.set(pct)
        if done:
            self._set_status(
                f"Done. Matches: {len(self.matches)} | Geocode calls: {geocode_calls} | "
                f"Skipped missing city/prov: {skipped_missing} | Skipped geocode: {skipped_geocode}"
            )
        else:
            self._set_status(f"Processing {current}/{total} leads... (new geocodes this run: {geocode_calls})")

    def _start_search(self):
        # Run in a thread so UI stays responsive
        t = threading.Thread(target=self._run_search, daemon=True)
        t.start()

    def _run_search(self):
        try:
            dealer_name = self.dealer_var.get().strip()
            dealer = find_dealer(dealer_name)

            try:
                radius = float(self.radius_var.get().strip())
            except ValueError:
                self._ui(messagebox.showerror("Invalid radius", "Radius must be a number."))
                return

            leads_path = Path(self.leads_path.get().strip())
            if not leads_path.exists():
                self._ui(messagebox.showerror("Missing file", f"Leads file not found:\n{leads_path}"))
                
                return

            leads = load_leads_export(leads_path)
            self.matches = []
            self._set_status("Loading leads...")

            matches = leads_within_radius(
                dealer=dealer,
                leads=leads,
                radius_km=radius,
                progress_cb=lambda c, t, g, **kw: self._ui(self._progress, c, t, g, **kw),

            )
            self.matches = matches

            # Populate table
            for item in self.tree.get_children():
                self.tree.delete(item)

            for r in self.matches:
                phone = ", ".join([x for x in [r.get("phone") or "", r.get("mobile") or ""] if x])
                prov = r.get("province_state_code") or r.get("province_state_name") or ""
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        r.get("distance_km", ""),
                        r.get("name", ""),
                        r.get("city", ""),
                        prov,
                        r.get("stage_name", ""),
                        phone,
                        r.get("email", ""),
                    ),
                )

            self.progress_var.set(100.0)

        except Exception as e:
            self._ui(messagebox.showerror, "Error", str(e))


    def _export_csv(self):
        if not self.matches:
            self._ui(messagebox.showinfo("No results", "Run a search first."))

            return

        path = filedialog.asksaveasfilename(
            title="Save CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not path:
            return

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["distance_km", "lead", "city", "province", "stage", "phone", "email"])
            for r in self.matches:
                phone = ", ".join([x for x in [r.get("phone") or "", r.get("mobile") or ""] if x])
                prov = r.get("province_state_code") or r.get("province_state_name") or ""
                w.writerow([r.get("distance_km", ""), r.get("name", ""), r.get("city", ""), prov, r.get("stage_name", ""), phone, r.get("email", "")])

        self._ui(messagebox.showinfo("Saved", f"CSV saved:\n{path}"))

    def _ui(self, fn, *args, **kwargs):
        """Run a callable on the Tk main thread."""
        self.after(0, lambda: fn(*args, **kwargs))

    def _open_dealer_picker(self):
        dealers = sorted([d.get("Location", "") for d in DEALER_LOCATIONS if d.get("Location")])

        win = tk.Toplevel(self)
        win.title("Select Co-op Location")
        win.geometry("520x420")
        win.transient(self)


        # Make it modal
        win.grab_set()
        self._activate_window(win)

        # If user closes the window via the red X, restore focus to main
        def on_close():
            try:
                win.grab_release()
            except Exception:
                pass
            win.destroy()
            self._restore_main_focus()


        win.protocol("WM_DELETE_WINDOW", on_close)

        # Search box
        search_var = tk.StringVar(value="")
        ttk.Label(win, text="Search:").pack(anchor="w", padx=10, pady=(10, 0))
        search_entry = ttk.Entry(win, textvariable=search_var)
        search_entry.pack(fill="x", padx=10)

        # Listbox with scrollbar
        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=10, pady=10)

        yscroll = ttk.Scrollbar(frame, orient="vertical")
        yscroll.pack(side="right", fill="y")

        lb = tk.Listbox(
            frame,
            yscrollcommand=yscroll.set,
            activestyle="dotbox",
            selectmode="browse",
            exportselection=False,
        )
        lb.pack(side="left", fill="both", expand=True)
        yscroll.config(command=lb.yview)

        def populate(filtered: List[str]):
            lb.delete(0, tk.END)
            for d in filtered:
                lb.insert(tk.END, d)

        populate(dealers)

        def on_search(*_):
            q = search_var.get().strip().lower()
            if not q:
                populate(dealers)
            else:
                populate([d for d in dealers if q in d.lower()])

        search_var.trace_add("write", on_search)

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=10, pady=(0, 10))

        def accept():
            sel = lb.curselection()
            if not sel:
                return
            value = lb.get(sel[0])
            self.dealer_var.set(value)
            on_close()  # closes + restores focus

        ttk.Button(btns, text="OK", command=accept).pack(side="right")
        ttk.Button(btns, text="Cancel", command=on_close).pack(side="right", padx=(0, 8))

        lb.bind("<Double-Button-1>", lambda e: accept())
        lb.bind("<Return>", lambda e: accept())
        lb.bind("<Escape>", lambda e: on_close())

        # Ensure keyboard focus starts in the search entry
        search_entry.focus_set()

        # Optional: block here until the dialog closes (more modal correctness)
        win.wait_window()

    def _activate_window(self, win: tk.Toplevel | tk.Tk):
        """Bring a window to front and ensure it has focus (macOS-friendly)."""
        try:
            win.lift()
        except Exception:
            pass
        try:
            win.attributes("-topmost", True)
            win.after(50, lambda: win.attributes("-topmost", False))
        except Exception:
            pass
        try:
            win.focus_force()
        except Exception:
            pass


    def _restore_main_focus(self):
        """Restore focus to the main window after closing a dialog."""
        self._activate_window(self)
        # Put keyboard focus somewhere sensible:
        try:
            self.focus_set()
        except Exception:
            pass

if __name__ == "__main__":
    App().mainloop()
