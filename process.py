#!/usr/bin/env python3
"""Thailand DLT car-registration ETL (flow-data source).

Reads gross new-registration counts from two DLT exports in
data/cars-data/:
  - 10663.xlsx -> brand × model × vehicle-type × province × month
  - 10662.xlsx -> brand × fuel  × vehicle-type × province × month

Aggregates to brand-level, model-level and fuel-level series, emits a
single NDJSON file, and renders a self-contained index.html.

Replaces the earlier cumulative-snapshot pipeline (the
RegisteredBrand_*_Car.xls files are kept on disk but no longer read).
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import openpyxl

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data" / "cars-data"
OUT_DIR = ROOT / "output"
HTML_PATH = ROOT / "index.html"
TEMPLATE_TOKEN = "__DATA_NDJSON__"

FILE_10663 = DATA_DIR / "10663.xlsx"  # brand/model
FILE_10662 = DATA_DIR / "10662.xlsx"  # brand/fuel

THAI_MONTHS = {
    "มกราคม": 1, "กุมภาพันธ์": 2, "มีนาคม": 3, "เมษายน": 4,
    "พฤษภาคม": 5, "มิถุนายน": 6, "กรกฎาคม": 7, "สิงหาคม": 8,
    "กันยายน": 9, "ตุลาคม": 10, "พฤศจิกายน": 11, "ธันวาคม": 12,
}

VEHICLE_CODE_RE = re.compile(r"^(รย\.\d+)")

# Brand spelling normalizations carried over from the previous pipeline.
BRAND_ALIASES = {
    "MERCEDES BENZ": "MERCEDES-BENZ",
    "ROLLS ROYCE": "ROLLS-ROYCE",
    "LANDROVER": "LAND ROVER",
    "M.G.": "MG",
}

# DLT exports include a long tail of internal SKU-style codes (e.g.
# "A03AXTHHRU", "KP3TEYJFPRU", "FD30NT", "GX3600", "MF61WD") that pollute
# the per-brand model lists. Heuristic: pure A–Z/0–9, 6+ chars, contains
# both a letter and a digit, no separators. Real model names typically
# contain spaces, dashes, dots, or parens; common short trade names like
# "ATTO3" (5 chars) and "F150" survive the 6-char floor.
CODE_MODEL_RE = re.compile(r"^[A-Z0-9]{6,}$")
UNSPECIFIED_MODELS = {"ไม่ระบุ", "(UNSPECIFIED)", ""}


def is_coded_model(name: str) -> bool:
    """Return True for DLT internal SKU-like codes and "unspecified" stubs."""
    if name in UNSPECIFIED_MODELS:
        return True
    if not CODE_MODEL_RE.fullmatch(name):
        return False  # has spaces / dashes / dots / parens -> looks like a real name
    return any(c.isdigit() for c in name) and any(c.isalpha() for c in name)


def be_to_ad(y) -> int:
    return int(y) - 543


def normalize_str(raw) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    return re.sub(r"\s+", " ", s)


def normalize_brand(raw) -> str | None:
    s = normalize_str(raw)
    if s is None:
        return None
    s = s.upper()
    return BRAND_ALIASES.get(s, s)


def parse_vehicle_type(raw) -> tuple[str, str] | tuple[None, None]:
    """Return (code, label). code is 'รย.1' style; label is the full string."""
    s = normalize_str(raw)
    if not s:
        return None, None
    m = VEHICLE_CODE_RE.match(s)
    if not m:
        return None, None
    return m.group(1), s


def stream_data_sheet(path: Path):
    """Yield rows from the 'Data' sheet, skipping header rows."""
    wb = openpyxl.load_workbook(str(path), data_only=True, read_only=True)
    try:
        ws = wb["Data"]
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i < 6:  # rows 0..5 are titles/notes/column-header
                continue
            if not row or row[0] is None:
                continue
            yield row
    finally:
        wb.close()


def read_10663(path: Path):
    """Return aggregates from 10663 (brand × model granularity).

    Returns dict with keys: brand_monthly, brand_yearly, model_monthly,
    model_yearly, brand_total, model_total, vtypes (code -> label).
    Counters are keyed by tuples and produce int totals.
    """
    print(f"Reading {path.name} ...")
    brand_m: dict[tuple, int] = defaultdict(int)   # (brand, vt, year, month)
    brand_y: dict[tuple, int] = defaultdict(int)   # (brand, vt, year)
    model_m: dict[tuple, int] = defaultdict(int)   # (brand, model, vt, year, month)
    model_y: dict[tuple, int] = defaultdict(int)   # (brand, model, vt, year)
    brand_total: dict[str, int] = defaultdict(int)
    model_total: dict[tuple[str, str], int] = defaultdict(int)
    vtypes: dict[str, str] = {}
    n_rows = 0
    dropped_rows = 0
    dropped_vehicles = 0
    dropped_model_names: set[str] = set()
    for row in stream_data_sheet(path):
        # Columns: ปี, เดือน, ประเภทรถ, รหัสจังหวัด, จังหวัด, ยี่ห้อรถ, รุ่นรถ, จำนวนรถ
        try:
            year_be, mon_th, vtype_raw, _pcode, _prov, brand_raw, model_raw, count = row[:8]
        except ValueError:
            continue
        if count is None:
            continue
        try:
            c = int(count)
        except (ValueError, TypeError):
            continue
        if c == 0:
            continue
        try:
            year = be_to_ad(year_be)
        except (ValueError, TypeError):
            continue
        month = THAI_MONTHS.get(str(mon_th).strip()) if mon_th else None
        if not month:
            continue
        vt_code, vt_label = parse_vehicle_type(vtype_raw)
        if not vt_code:
            continue
        brand = normalize_brand(brand_raw)
        if not brand:
            continue

        # Model normalization: collapse whitespace + uppercase, so that
        # "Hilux Revo" and "HILUX REVO" merge into a single bucket.
        model = normalize_str(model_raw)
        if model is None:
            model = ""
        model = model.upper()

        # Brand-level aggregates always include every row, so brand totals
        # stay consistent with 10662 (which has no model granularity).
        vtypes[vt_code] = vt_label
        brand_m[(brand, vt_code, year, month)] += c
        brand_y[(brand, vt_code, year)] += c
        brand_total[brand] += c
        n_rows += 1

        # Model-level aggregates exclude DLT internal codes / unspecified
        # stub rows so the Models tab isn't polluted by SKU gibberish.
        if is_coded_model(model):
            dropped_rows += 1
            dropped_vehicles += c
            dropped_model_names.add(model)
            continue

        model_m[(brand, model, vt_code, year, month)] += c
        model_y[(brand, model, vt_code, year)] += c
        model_total[(brand, model)] += c
    print(f"  {n_rows:,} data rows aggregated  ({len(brand_total):,} brands, "
          f"{len(model_total):,} models, {len(vtypes)} vehicle types)")
    if dropped_rows:
        print(f"  Dropped {dropped_rows:,} coded/unspecified model rows "
              f"({len(dropped_model_names):,} distinct names, "
              f"{dropped_vehicles:,} vehicles)")
    return {
        "brand_monthly": brand_m,
        "brand_yearly":  brand_y,
        "model_monthly": model_m,
        "model_yearly":  model_y,
        "brand_total":   brand_total,
        "model_total":   model_total,
        "vtypes":        vtypes,
    }


def read_10662(path: Path):
    """Return brand×fuel aggregates from 10662."""
    print(f"Reading {path.name} ...")
    fuel_m: dict[tuple, int] = defaultdict(int)    # (brand, fuel, vt, year, month)
    fuel_y: dict[tuple, int] = defaultdict(int)    # (brand, fuel, vt, year)
    brand_y: dict[tuple, int] = defaultdict(int)   # (brand, vt, year)   -- for cross-check
    fuels: set[str] = set()
    n_rows = 0
    for row in stream_data_sheet(path):
        # Columns: ปี, เดือน, ประเภทรถ, รหัสจังหวัด, จังหวัด, ยี่ห้อรถ, ชนิดเชื้อเพลิง, จำนวนรถ
        try:
            year_be, mon_th, vtype_raw, _pcode, _prov, brand_raw, fuel_raw, count = row[:8]
        except ValueError:
            continue
        if count is None:
            continue
        try:
            c = int(count)
        except (ValueError, TypeError):
            continue
        if c == 0:
            continue
        try:
            year = be_to_ad(year_be)
        except (ValueError, TypeError):
            continue
        month = THAI_MONTHS.get(str(mon_th).strip()) if mon_th else None
        if not month:
            continue
        vt_code, _ = parse_vehicle_type(vtype_raw)
        if not vt_code:
            continue
        brand = normalize_brand(brand_raw)
        if not brand:
            continue
        fuel = normalize_str(fuel_raw) or "(unspecified)"

        fuels.add(fuel)
        fuel_m[(brand, fuel, vt_code, year, month)] += c
        fuel_y[(brand, fuel, vt_code, year)] += c
        brand_y[(brand, vt_code, year)] += c
        n_rows += 1
    print(f"  {n_rows:,} data rows aggregated  ({len(fuels)} fuel types)")
    return {
        "fuel_monthly":   fuel_m,
        "fuel_yearly":    fuel_y,
        "brand_yearly":   brand_y,
        "fuels":          fuels,
    }


def cross_check(b663: dict, b662: dict, tolerance: int = 100) -> int:
    """Compare brand-yearly totals between the two sources."""
    a = b663
    b = b662
    keys = set(a) | set(b)
    discrepancies = 0
    biggest = []
    for k in keys:
        va = a.get(k, 0)
        vb = b.get(k, 0)
        if abs(va - vb) > tolerance:
            discrepancies += 1
            biggest.append((abs(va - vb), k, va, vb))
    if discrepancies == 0:
        print("OK: 10663 and 10662 brand-yearly totals agree (within tolerance).")
    else:
        biggest.sort(reverse=True)
        print(f"WARN: {discrepancies:,} brand-yearly key(s) differ between 10663 and 10662 "
              f"beyond ±{tolerance}. Top 10:")
        for diff, k, va, vb in biggest[:10]:
            print(f"   {k}  10663={va:,}  10662={vb:,}  Δ={va-vb:+,}")
    return discrepancies


def emit_records(b663: dict, b662: dict) -> list[dict]:
    out: list[dict] = []
    for (brand, vt, year, month), n in b663["brand_monthly"].items():
        out.append({"kind": "brand_m", "brand": brand, "vt": vt,
                    "year": year, "month": month, "n": n})
    for (brand, vt, year), n in b663["brand_yearly"].items():
        out.append({"kind": "brand_y", "brand": brand, "vt": vt,
                    "year": year, "n": n})
    for (brand, model, vt, year, month), n in b663["model_monthly"].items():
        out.append({"kind": "model_m", "brand": brand, "model": model,
                    "vt": vt, "year": year, "month": month, "n": n})
    for (brand, model, vt, year), n in b663["model_yearly"].items():
        out.append({"kind": "model_y", "brand": brand, "model": model,
                    "vt": vt, "year": year, "n": n})
    for (brand, fuel, vt, year, month), n in b662["fuel_monthly"].items():
        out.append({"kind": "fuel_m", "brand": brand, "fuel": fuel,
                    "vt": vt, "year": year, "month": month, "n": n})
    for (brand, fuel, vt, year), n in b662["fuel_yearly"].items():
        out.append({"kind": "fuel_y", "brand": brand, "fuel": fuel,
                    "vt": vt, "year": year, "n": n})
    return out


def emit_meta(b663: dict, b662: dict) -> dict:
    """Static lookup tables baked into the page for the UI to consume."""
    vt_list = [
        {"code": code, "label": label}
        for code, label in sorted(b663["vtypes"].items(), key=lambda kv: (
            int(re.search(r"\d+", kv[0]).group()) if re.search(r"\d+", kv[0]) else 99,
            kv[0],
        ))
    ]
    brand_totals = [
        {"brand": b, "total": t}
        for b, t in sorted(b663["brand_total"].items(),
                           key=lambda kv: (-kv[1], kv[0]))
    ]
    model_totals = [
        {"brand": b, "model": m, "total": t}
        for (b, m), t in sorted(b663["model_total"].items(),
                                key=lambda kv: (-kv[1], kv[0][0], kv[0][1]))
    ]
    fuels = sorted(b662["fuels"])
    return {
        "vehicle_types": vt_list,
        "brand_totals":  brand_totals,
        "model_totals":  model_totals,
        "fuels":         fuels,
    }


def write_ndjson(records: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def main() -> int:
    if not FILE_10663.exists():
        print(f"ERROR: missing {FILE_10663}", file=sys.stderr)
        return 1
    if not FILE_10662.exists():
        print(f"ERROR: missing {FILE_10662}", file=sys.stderr)
        return 1

    b663 = read_10663(FILE_10663)
    b662 = read_10662(FILE_10662)

    cross_check(b663["brand_yearly"], b662["brand_yearly"])

    records = emit_records(b663, b662)
    meta = emit_meta(b663, b662)

    # Sort for stable output
    records.sort(key=lambda r: (r["kind"], r.get("brand", ""),
                                r.get("model", ""), r.get("fuel", ""),
                                r.get("vt", ""), r.get("year", 0),
                                r.get("month", 0)))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ndjson_path = OUT_DIR / "data.ndjson"
    write_ndjson(records, ndjson_path)

    # Build embedded JS: { meta: {...}, records: [...] }
    payload = {
        "meta": meta,
        "records": records,
    }
    payload_js = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = HTML_TEMPLATE.replace(TEMPLATE_TOKEN, payload_js)
    HTML_PATH.write_text(html, encoding="utf-8")

    # Summary
    by_kind: dict[str, int] = defaultdict(int)
    for r in records:
        by_kind[r["kind"]] += 1
    monthly_keys = sorted({(r["year"], r["month"])
                           for r in records if r["kind"] == "brand_m"})
    yearly_keys = sorted({r["year"] for r in records if r["kind"] == "brand_y"})

    print()
    print("=" * 60)
    print(f"Total records: {len(records):,}")
    for k, n in sorted(by_kind.items()):
        print(f"  {k:<10s} {n:>10,}")
    print(f"Brands:       {len(meta['brand_totals']):,}")
    print(f"Models:       {len(meta['model_totals']):,}")
    print(f"Vehicle types:{len(meta['vehicle_types']):>3}")
    print(f"Fuels:        {len(meta['fuels']):>3}")
    if monthly_keys:
        print(f"Monthly range: {monthly_keys[0][0]}-{monthly_keys[0][1]:02d}"
              f"  ..  {monthly_keys[-1][0]}-{monthly_keys[-1][1]:02d}")
    if yearly_keys:
        print(f"Yearly range:  {yearly_keys[0]}  ..  {yearly_keys[-1]}")
    print(f"Wrote {ndjson_path}  ({ndjson_path.stat().st_size:,} bytes)")
    print(f"Wrote {HTML_PATH}    ({HTML_PATH.stat().st_size:,} bytes)")
    return 0


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Thailand new car registrations (DLT)</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #fafafa; --fg: #222; --muted: #666; --border: #e0e0e0;
    --accent: #1d4ed8; --accent-bg: #eff6ff;
  }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: var(--bg); color: var(--fg); margin: 0; padding: 16px; font-size: 14px; }
  h1 { font-size: 20px; margin: 0 0 4px 0; }
  .subtitle { color: var(--muted); font-size: 12px; margin-bottom: 12px; }
  .app { display: grid; grid-template-columns: 280px 1fr; gap: 16px; }
  .sidebar { background: white; border: 1px solid var(--border); border-radius: 8px;
             padding: 10px; max-height: 88vh; overflow-y: auto; }
  .section { margin-bottom: 14px; }
  .section h2 { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em;
                color: var(--muted); margin: 0 0 6px 0; }
  .btnrow { display: flex; gap: 4px; margin-bottom: 6px; }
  .btnrow button { flex: 1; font-size: 11px; padding: 4px; border: 1px solid var(--border);
                   background: white; border-radius: 4px; cursor: pointer; }
  .btnrow button:hover { background: var(--accent-bg); }
  .btnrow button.active { background: var(--accent); color: white; border-color: var(--accent); }
  label.row { font-size: 13px; display: flex; align-items: center; gap: 6px;
              cursor: pointer; padding: 1px 4px; border-radius: 3px; }
  label.row:hover { background: var(--accent-bg); }
  label.row .swatch { width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }
  label.row .count { margin-left: auto; color: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
  .vt-list, .brand-list { display: flex; flex-direction: column; gap: 2px; }
  .vt-group-heading { font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em;
                      color: var(--muted); margin: 6px 0 2px 0; padding-left: 4px; }
  .vt-group-heading:first-child { margin-top: 0; }
  input.search { width: 100%; padding: 4px 6px; border: 1px solid var(--border);
                 border-radius: 4px; font-size: 12px; margin-bottom: 4px; }
  details.brand { border-left: 2px solid var(--border); padding-left: 6px; margin-bottom: 2px; }
  details.brand[open] { border-left-color: var(--accent); }
  details.brand > summary { cursor: pointer; padding: 2px 4px; font-size: 13px;
                            display: flex; align-items: center; gap: 6px; }
  details.brand > summary:hover { background: var(--accent-bg); }
  details.brand > summary .label { flex: 1; }
  details.brand > summary .count { color: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
  details.brand > .models { margin-left: 12px; display: flex; flex-direction: column; gap: 1px; padding: 2px 0; }
  details.brand > .models a.mini { font-size: 10px; color: var(--accent); cursor: pointer; margin-bottom: 2px; }
  .main { background: white; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
  .tabs { display: flex; gap: 4px; margin-bottom: 8px; border-bottom: 1px solid var(--border); }
  .tabs button { background: none; border: none; padding: 8px 14px; font-size: 14px; cursor: pointer;
                 color: var(--muted); border-bottom: 2px solid transparent; margin-bottom: -1px; }
  .tabs button.active { color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; }
  .sub-toggle { display: flex; gap: 4px; margin-bottom: 10px; }
  .sub-toggle button { font-size: 12px; padding: 3px 10px; border: 1px solid var(--border);
                       background: white; border-radius: 4px; cursor: pointer; color: var(--muted); }
  .sub-toggle button.active { background: var(--accent); color: white; border-color: var(--accent); }
  .chart-wrap { position: relative; height: 70vh; }
  .footer { color: var(--muted); font-size: 11px; margin-top: 8px; }
  .pill { display: inline-block; font-size: 10px; padding: 1px 6px; border-radius: 10px;
          background: var(--accent-bg); color: var(--accent); margin-left: 6px; }
</style>
</head>
<body>
  <h1>Thailand new car registrations</h1>
  <div class="subtitle">Source: Department of Land Transport (กรมการขนส่งทางบก), reports 10662 + 10663. Gross new registrations; excludes used-vehicle re-registrations.</div>
  <div class="app">
    <aside class="sidebar">
      <div class="section">
        <h2>Vehicle types</h2>
        <div class="btnrow">
          <button data-vt="cars" class="active">Cars</button>
          <button data-vt="motorcycles">Motorcycles</button>
          <button data-vt="all">All</button>
          <button data-vt="none">None</button>
        </div>
        <div class="vt-list" id="vtList"></div>
      </div>

      <div class="section" id="brandSection">
        <h2>Brands</h2>
        <div class="btnrow">
          <button id="brandSelectAll">Select all</button>
          <button id="brandClearAll">Clear</button>
        </div>
        <input class="search" id="brandSearch" placeholder="Search brand…">
        <div class="brand-list" id="brandList"></div>
      </div>

      <div class="section" id="modelSection" style="display:none">
        <h2>Models (brand → model)</h2>
        <input class="search" id="modelSearch" placeholder="Search brand or model…">
        <div id="modelTree"></div>
      </div>
    </aside>

    <section class="main">
      <div class="tabs">
        <button id="tabMonthly" class="active">Monthly</button>
        <button id="tabYearly">Yearly</button>
        <button id="tabModels">Models</button>
      </div>
      <div class="sub-toggle" id="modelSubToggle" style="display:none">
        <button id="modelSubMonthly" class="active">Monthly</button>
        <button id="modelSubYearly">Yearly</button>
      </div>
      <div class="chart-wrap"><canvas id="chart"></canvas></div>
      <div class="footer" id="footer"></div>
    </section>
  </div>

<script>
const PAYLOAD = __DATA_NDJSON__;
const META = PAYLOAD.meta;
const RECORDS = PAYLOAD.records;

// ---------- palette ----------
const PALETTE = [
  "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
  "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf",
  "#aec7e8","#ffbb78","#98df8a","#ff9896","#c5b0d5",
  "#c49c94","#f7b6d2","#c7c7c7","#dbdb8d","#9edae5"
];
function hashIdx(s, mod) {
  let h = 0;
  for (let i=0; i<s.length; i++) h = (h*31 + s.charCodeAt(i)) | 0;
  return Math.abs(h) % mod;
}
const brandColor = {};
META.brand_totals.forEach((b, i) => { brandColor[b.brand] = PALETTE[i % PALETTE.length]; });
function modelColor(brand, model, idx) {
  // base hue from brand, vary lightness by model index
  const base = brandColor[brand] || PALETTE[hashIdx(brand, PALETTE.length)];
  // shift via hex math is hard; we'll just rotate the palette by model idx for clarity
  return PALETTE[(PALETTE.indexOf(base) + idx + 1) % PALETTE.length] || base;
}

// ---------- vehicle-type English labels ----------
// The full Thai label is too long to display inline, so we show a
// concise English label with the รย.X code acting as the Thai
// abbreviation. The full Thai description appears as a hover tooltip.
const VT_EN_LABELS = {
  "รย.1":  "Passenger car ≤7 seats",
  "รย.2":  "Passenger car >7 seats",
  "รย.3":  "Personal pickup/truck",
  "รย.4":  "Personal 3-wheel car",
  "รย.6":  "Taxi ≤7 passengers",
  "รย.7":  "Small 4-wheel for-hire",
  "รย.8":  "For-hire 3-wheel",
  "รย.9":  "Business service car",
  "รย.10": "Tourist service car",
  "รย.11": "Rental service car",
  "รย.12": "Motorcycle",
  "รย.13": "Tractor",
  "รย.14": "Road roller",
  "รย.15": "Agricultural vehicle",
  "รย.16": "Trailer",
  "รย.17": "Public motorcycle",
};

// ---------- vehicle-type groups (presentation only) ----------
const VT_GROUPS = [
  { name: "Cars",            codes: ["รย.1", "รย.2"] },
  { name: "Pickups & trucks", codes: ["รย.3", "รย.4"] },
  { name: "For-hire / taxi",  codes: ["รย.6", "รย.7", "รย.8"] },
  { name: "Service",          codes: ["รย.9", "รย.10", "รย.11"] },
  { name: "Motorcycles",      codes: ["รย.12", "รย.17"] },
  { name: "Other",            codes: ["รย.13", "รย.14", "รย.15", "รย.16"] },
];
const PRESETS = {
  cars:        new Set(["รย.1", "รย.2"]),
  motorcycles: new Set(["รย.12", "รย.17"]),
};

// ---------- state ----------
const vtSelected = new Set(PRESETS.cars);

const BRAND_LIST = META.brand_totals.map(b => b.brand);
const BRAND_TOTAL = Object.fromEntries(META.brand_totals.map(b => [b.brand, b.total]));
const TOP_N_BRANDS = 10;
const TOP_N_MODELS = 5;
const brandSelected = new Set(BRAND_LIST.slice(0, TOP_N_BRANDS));

// Map: brand -> [{model, total}, …] sorted desc
const MODELS_BY_BRAND = {};
for (const r of META.model_totals) {
  if (!MODELS_BY_BRAND[r.brand]) MODELS_BY_BRAND[r.brand] = [];
  MODELS_BY_BRAND[r.brand].push({ model: r.model, total: r.total });
}

// Default model selection: empty — user picks from the tree
const modelSelected = new Set();

let currentTab = "monthly";
let modelView = "monthly"; // sub-toggle in Models tab
let chart = null;

// ---------- index records for fast access ----------
const monthlyKeys = (() => {
  const s = new Set();
  for (const r of RECORDS) if (r.kind === "brand_m") s.add(r.year * 100 + r.month);
  return [...s].sort((a,b) => a-b);
})();
const monthlyLabels = monthlyKeys.map(k => {
  const y = Math.floor(k/100), m = k % 100;
  return `${y}-${String(m).padStart(2,'0')}`;
});
const yearlyKeys = (() => {
  const s = new Set();
  for (const r of RECORDS) if (r.kind === "brand_y") s.add(r.year);
  return [...s].sort((a,b) => a-b);
})();
const latestPeriod = monthlyKeys.length ? monthlyKeys[monthlyKeys.length-1] : null;
const latestYear = latestPeriod ? Math.floor(latestPeriod / 100) : null;
const latestMonth = latestPeriod ? latestPeriod % 100 : null;
const yearlyLabels = yearlyKeys.map(y => (y === latestYear && latestMonth !== 12) ? `${y} YTD` : String(y));

// Group records by kind into pre-built maps keyed for lookup
function buildIndex() {
  // brand_m: brand -> vt -> "YYYYMM" -> n
  const bm = {}, by = {}, mm = {}, my = {};
  for (const r of RECORDS) {
    if (r.kind === "brand_m") {
      const k = r.year*100 + r.month;
      ((bm[r.brand] ??= {})[r.vt] ??= {})[k] = r.n;
    } else if (r.kind === "brand_y") {
      ((by[r.brand] ??= {})[r.vt] ??= {})[r.year] = r.n;
    } else if (r.kind === "model_m") {
      const k = r.year*100 + r.month;
      const tag = `${r.brand}|${r.model}`;
      ((mm[tag] ??= {})[r.vt] ??= {})[k] = r.n;
    } else if (r.kind === "model_y") {
      const tag = `${r.brand}|${r.model}`;
      ((my[tag] ??= {})[r.vt] ??= {})[r.year] = r.n;
    }
  }
  return { bm, by, mm, my };
}
const IDX = buildIndex();

function sumOverVt(perVtMap, vtFilter, periodKeys) {
  // perVtMap: vt -> period -> n. Returns array aligned with periodKeys.
  const out = periodKeys.map(() => 0);
  if (!perVtMap) return out;
  for (const vt of Object.keys(perVtMap)) {
    if (vtFilter.size && !vtFilter.has(vt)) continue;
    const series = perVtMap[vt];
    for (let i=0; i<periodKeys.length; i++) {
      const v = series[periodKeys[i]];
      if (v) out[i] += v;
    }
  }
  return out;
}

// ---------- rendering ----------
function renderVtList() {
  const list = document.getElementById('vtList');
  list.innerHTML = '';
  // Build a lookup of full labels for each known code
  const labelByCode = {};
  for (const v of META.vehicle_types) labelByCode[v.code] = v.label;
  // Render each group with a heading
  const placedCodes = new Set();
  for (const group of VT_GROUPS) {
    const codesInData = group.codes.filter(c => labelByCode[c]);
    if (codesInData.length === 0) continue;
    const h = document.createElement('div');
    h.className = 'vt-group-heading';
    h.textContent = group.name;
    list.append(h);
    for (const code of codesInData) {
      placedCodes.add(code);
      list.append(buildVtRow(code, labelByCode[code]));
    }
  }
  // Catch any codes not assigned to a group (shouldn't happen but defensive)
  const orphans = META.vehicle_types.filter(v => !placedCodes.has(v.code));
  if (orphans.length) {
    const h = document.createElement('div');
    h.className = 'vt-group-heading';
    h.textContent = "Uncategorised";
    list.append(h);
    for (const v of orphans) list.append(buildVtRow(v.code, v.label));
  }
}

function buildVtRow(code, fullLabel) {
  const lab = document.createElement('label');
  lab.className = 'row';
  // Tooltip = the full Thai description from DLT
  const thaiOnly = (fullLabel || '').replace(/^รย\.\d+\s*/, '').trim();
  if (thaiOnly) lab.title = `${code} — ${thaiOnly}`;
  const cb = document.createElement('input');
  cb.type = 'checkbox';
  cb.checked = vtSelected.has(code);
  cb.addEventListener('change', () => {
    if (cb.checked) vtSelected.add(code); else vtSelected.delete(code);
    updateVtPresetButtons();
    render();
  });
  const codeSpan = document.createElement('span');
  codeSpan.style.minWidth = '32px';
  codeSpan.style.color = 'var(--muted)';
  codeSpan.style.fontSize = '11px';
  codeSpan.textContent = code; // รย.X — serves as the Thai abbreviation
  const labelSpan = document.createElement('span');
  labelSpan.textContent = VT_EN_LABELS[code] || thaiOnly || code;
  labelSpan.style.fontSize = '12px';
  lab.append(cb, codeSpan, labelSpan);
  return lab;
}

function setsEqual(a, b) {
  if (a.size !== b.size) return false;
  for (const x of a) if (!b.has(x)) return false;
  return true;
}

function updateVtPresetButtons() {
  const buttons = document.querySelectorAll('[data-vt]');
  buttons.forEach(b => b.classList.remove('active'));
  if (vtSelected.size === 0) {
    document.querySelector('[data-vt="none"]').classList.add('active');
  } else if (vtSelected.size === META.vehicle_types.length) {
    document.querySelector('[data-vt="all"]').classList.add('active');
  } else if (setsEqual(vtSelected, PRESETS.cars)) {
    document.querySelector('[data-vt="cars"]').classList.add('active');
  } else if (setsEqual(vtSelected, PRESETS.motorcycles)) {
    document.querySelector('[data-vt="motorcycles"]').classList.add('active');
  }
}

function renderBrandList(filter) {
  const list = document.getElementById('brandList');
  list.innerHTML = '';
  const q = (filter || '').trim().toLowerCase();
  for (const b of BRAND_LIST) {
    if (q && !b.toLowerCase().includes(q)) continue;
    const lab = document.createElement('label');
    lab.className = 'row';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = brandSelected.has(b);
    cb.addEventListener('change', () => {
      if (cb.checked) brandSelected.add(b); else brandSelected.delete(b);
      render();
    });
    const sw = document.createElement('span');
    sw.className = 'swatch';
    sw.style.background = brandColor[b];
    const t = document.createElement('span');
    t.textContent = b;
    t.style.flex = '1';
    const c = document.createElement('span');
    c.className = 'count';
    c.textContent = BRAND_TOTAL[b].toLocaleString();
    lab.append(cb, sw, t, c);
    list.append(lab);
  }
}

function renderModelTree(filter) {
  const root = document.getElementById('modelTree');
  root.innerHTML = '';
  const q = (filter || '').trim().toLowerCase();
  const topSet = new Set(BRAND_LIST.slice(0, TOP_N_BRANDS));
  for (const b of BRAND_LIST) {
    const ms = MODELS_BY_BRAND[b] || [];
    const matchBrand = !q || b.toLowerCase().includes(q);
    // When searching: keep only models whose name matches (or all if brand matches)
    const filteredMs = q
      ? (matchBrand ? ms : ms.filter(m => m.model.toLowerCase().includes(q)))
      : ms;
    if (q && !matchBrand && filteredMs.length === 0) continue;

    const det = document.createElement('details');
    det.className = 'brand';
    // Defaults: top-10 brands pre-open; search auto-opens any matching brand;
    // everything else stays collapsed until clicked.
    det.open = !!q || topSet.has(b);

    const sum = document.createElement('summary');
    const sw = document.createElement('span');
    sw.className = 'swatch';
    sw.style.background = brandColor[b];
    const label = document.createElement('span');
    label.className = 'label';
    label.innerHTML = `<strong>${b}</strong> <span style="color:var(--muted);font-size:11px">(${ms.length})</span>`;
    const ct = document.createElement('span');
    ct.className = 'count';
    ct.textContent = (BRAND_TOTAL[b] || 0).toLocaleString();
    sum.append(sw, label, ct);
    det.append(sum);

    const wrap = document.createElement('div');
    wrap.className = 'models';

    const mini = document.createElement('div');
    mini.style.display = 'flex'; mini.style.gap = '8px';
    const linkAll = document.createElement('a'); linkAll.className = 'mini'; linkAll.textContent = 'check all';
    const linkNone = document.createElement('a'); linkNone.className = 'mini'; linkNone.textContent = 'clear';
    linkAll.addEventListener('click', () => {
      for (const m of filteredMs) modelSelected.add(`${b}|${m.model}`);
      renderModelTree(filter);
      render();
    });
    linkNone.addEventListener('click', () => {
      for (const m of filteredMs) modelSelected.delete(`${b}|${m.model}`);
      renderModelTree(filter);
      render();
    });
    mini.append(linkAll, linkNone);
    wrap.append(mini);

    // No truncation — show every model for this brand.
    for (const m of filteredMs) {
      const tag = `${b}|${m.model}`;
      const lab = document.createElement('label');
      lab.className = 'row';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.checked = modelSelected.has(tag);
      cb.addEventListener('change', () => {
        if (cb.checked) modelSelected.add(tag); else modelSelected.delete(tag);
        render();
      });
      const t = document.createElement('span');
      t.textContent = m.model;
      t.style.flex = '1';
      t.style.fontSize = '12px';
      const c = document.createElement('span');
      c.className = 'count';
      c.textContent = m.total.toLocaleString();
      lab.append(cb, t, c);
      wrap.append(lab);
    }
    det.append(wrap);
    root.append(det);
  }
}

function buildBrandDatasets(periodKeys, getMap) {
  const out = [];
  for (const b of BRAND_LIST) {
    if (!brandSelected.has(b)) continue;
    const series = getMap(b);
    if (!series) continue;
    const data = sumOverVt(series, vtSelected, periodKeys);
    if (data.every(v => v === 0)) continue;
    out.push({
      label: b,
      data,
      borderColor: brandColor[b],
      backgroundColor: brandColor[b],
      tension: 0.2,
      borderWidth: 2,
      pointRadius: 2,
    });
  }
  return out;
}

function buildModelDatasets(periodKeys, mapByTag) {
  const out = [];
  // Sort tags by total volume (desc) so colors stay consistent
  const tags = [...modelSelected];
  tags.sort((a, b) => {
    const ta = a.split('|'); const tb = b.split('|');
    const va = (META.model_totals.find(r => r.brand === ta[0] && r.model === ta[1]) || {}).total || 0;
    const vb = (META.model_totals.find(r => r.brand === tb[0] && r.model === tb[1]) || {}).total || 0;
    return vb - va;
  });
  const colorCounter = {};
  for (const tag of tags) {
    const [brand, model] = tag.split('|');
    const series = mapByTag[tag];
    if (!series) continue;
    const data = sumOverVt(series, vtSelected, periodKeys);
    if (data.every(v => v === 0)) continue;
    const i = colorCounter[brand] || 0;
    colorCounter[brand] = i + 1;
    out.push({
      label: `${brand} · ${model}`,
      data,
      borderColor: modelColor(brand, model, i),
      backgroundColor: modelColor(brand, model, i),
      tension: 0.2,
      borderWidth: 2,
      pointRadius: 2,
    });
  }
  return out;
}

function render() {
  if (chart) chart.destroy();
  const ctx = document.getElementById('chart').getContext('2d');
  let datasets, labels;
  if (currentTab === "monthly") {
    labels = monthlyLabels;
    datasets = buildBrandDatasets(monthlyKeys, (b) => IDX.bm[b]);
  } else if (currentTab === "yearly") {
    labels = yearlyLabels;
    datasets = buildBrandDatasets(yearlyKeys, (b) => IDX.by[b]);
  } else {
    if (modelView === "monthly") {
      labels = monthlyLabels;
      datasets = buildModelDatasets(monthlyKeys, IDX.mm);
    } else {
      labels = yearlyLabels;
      datasets = buildModelDatasets(yearlyKeys, IDX.my);
    }
  }

  chart = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false, axis: 'x' },
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toLocaleString()}`,
          },
        },
      },
      scales: {
        y: { beginAtZero: true, ticks: { callback: (v) => v.toLocaleString() },
             title: { display: true, text: "New registrations" } },
        x: { title: { display: true, text: currentTab === "yearly" || modelView === "yearly" ? "Year" : "Month" } },
      },
    },
  });

  // footer
  const f = document.getElementById('footer');
  let vts;
  if (vtSelected.size === META.vehicle_types.length) {
    vts = "all vehicle types";
  } else if (vtSelected.size === 0) {
    vts = "no vehicle types";
  } else if (setsEqual(vtSelected, PRESETS.cars)) {
    vts = "Cars (รย.1+2)";
  } else if (setsEqual(vtSelected, PRESETS.motorcycles)) {
    vts = "Motorcycles (รย.12+17)";
  } else if (vtSelected.size <= 3) {
    vts = [...vtSelected]
      .map(c => `${VT_EN_LABELS[c] || c} (${c})`)
      .join(", ");
  } else {
    vts = [...vtSelected].join(", ");
  }
  if (currentTab === "models") {
    f.textContent = `${datasets.length} model series · vehicle types: ${vts} · ${labels[0]} → ${labels[labels.length-1]}`;
  } else {
    f.textContent = `${datasets.length} of ${BRAND_LIST.length} brands · vehicle types: ${vts} · ${labels[0]} → ${labels[labels.length-1]}`;
  }
}

// ---------- wire up ----------
renderVtList();
renderBrandList();
renderModelTree();
updateVtPresetButtons();

document.querySelectorAll('[data-vt]').forEach(btn => {
  btn.addEventListener('click', () => {
    const k = btn.dataset.vt;
    vtSelected.clear();
    if (k === 'cars') for (const v of PRESETS.cars) vtSelected.add(v);
    else if (k === 'motorcycles') for (const v of PRESETS.motorcycles) vtSelected.add(v);
    else if (k === 'all') for (const v of META.vehicle_types) vtSelected.add(v.code);
    // 'none' leaves it empty
    renderVtList();
    updateVtPresetButtons();
    render();
  });
});

document.getElementById('brandSelectAll').addEventListener('click', () => {
  for (const b of BRAND_LIST) brandSelected.add(b);
  renderBrandList(document.getElementById('brandSearch').value);
  render();
});
document.getElementById('brandClearAll').addEventListener('click', () => {
  brandSelected.clear();
  renderBrandList(document.getElementById('brandSearch').value);
  render();
});
document.getElementById('brandSearch').addEventListener('input', (e) => {
  renderBrandList(e.target.value);
});

document.getElementById('modelSearch').addEventListener('input', (e) => {
  renderModelTree(e.target.value);
});

function switchTab(tab) {
  currentTab = tab;
  document.getElementById('tabMonthly').classList.toggle('active', tab === 'monthly');
  document.getElementById('tabYearly').classList.toggle('active', tab === 'yearly');
  document.getElementById('tabModels').classList.toggle('active', tab === 'models');
  document.getElementById('brandSection').style.display = (tab === 'models') ? 'none' : '';
  document.getElementById('modelSection').style.display = (tab === 'models') ? '' : 'none';
  document.getElementById('modelSubToggle').style.display = (tab === 'models') ? 'flex' : 'none';
  render();
}
document.getElementById('tabMonthly').addEventListener('click', () => switchTab('monthly'));
document.getElementById('tabYearly').addEventListener('click', () => switchTab('yearly'));
document.getElementById('tabModels').addEventListener('click', () => switchTab('models'));

document.getElementById('modelSubMonthly').addEventListener('click', () => {
  modelView = 'monthly';
  document.getElementById('modelSubMonthly').classList.add('active');
  document.getElementById('modelSubYearly').classList.remove('active');
  render();
});
document.getElementById('modelSubYearly').addEventListener('click', () => {
  modelView = 'yearly';
  document.getElementById('modelSubYearly').classList.add('active');
  document.getElementById('modelSubMonthly').classList.remove('active');
  render();
});

render();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main())
