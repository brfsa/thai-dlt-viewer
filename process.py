#!/usr/bin/env python3
"""Thailand DLT car registration ETL.

Reads cumulative-snapshot XLS/XLSX files from ./data/, computes new-registration
deltas, writes NDJSON, and renders a self-contained index.html.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import openpyxl
import xlrd

DATA_DIR = Path(__file__).parent / "data"
OUT_DIR = Path(__file__).parent / "output"
TEMPLATE_TOKEN = "__DATA_NDJSON__"

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}
FILENAME_RE = re.compile(r"^RegisteredBrand_(\d{1,2})([A-Za-z]{3})(\d{2})_Car\.xlsx?$")

BRAND_ALIASES = {
    "MERCEDES BENZ": "MERCEDES-BENZ",
    "ROLLS ROYCE": "ROLLS-ROYCE",
    "LANDROVER": "LAND ROVER",
    "M.G.": "MG",
}

THAI_RANGE = ("฀", "๿")


def has_thai(s: str) -> bool:
    return any(THAI_RANGE[0] <= ch <= THAI_RANGE[1] for ch in s)


def normalize_brand(raw) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).upper()
    return BRAND_ALIASES.get(s, s)


def is_filter_row(raw_brand) -> bool:
    if raw_brand is None:
        return True
    s = str(raw_brand).strip()
    if not s:
        return True
    if s.startswith("รวม"):
        return True
    if "ยี่ห้ออื่น" in s:
        return True
    if s == "ไม่ระบุ":
        return True
    if s.startswith("กลุ่ม"):
        return True
    return False


def discover_files() -> list[tuple[int, int, Path]]:
    entries = []
    for p in DATA_DIR.iterdir():
        m = FILENAME_RE.match(p.name)
        if not m:
            continue
        _, mon_abbr, yy = m.groups()
        mon_abbr = mon_abbr.capitalize()
        if mon_abbr not in MONTHS:
            continue
        year = 2000 + int(yy)
        month = MONTHS[mon_abbr]
        entries.append((year, month, p))
    entries.sort(key=lambda e: (e[0], e[1]))
    return entries


def read_xls(path: Path) -> dict[str, dict[str, float]]:
    """Return {sheet_name: {brand: cumulative}}."""
    book = xlrd.open_workbook(str(path))
    out: dict[str, dict[str, float]] = {}
    for sn in ("รย.1", "รย.2"):
        if sn not in book.sheet_names():
            continue
        s = book.sheet_by_name(sn)
        sheet_data: dict[str, float] = {}
        for r in range(5, s.nrows):
            raw_brand = s.cell_value(r, 1) if s.ncols > 1 else None
            if is_filter_row(raw_brand):
                continue
            try:
                count = float(s.cell_value(r, 2))
            except (ValueError, IndexError):
                continue
            brand = normalize_brand(raw_brand)
            if brand is None:
                continue
            sheet_data[brand] = sheet_data.get(brand, 0.0) + count
        out[sn] = sheet_data
    return out


def read_xlsx(path: Path) -> dict[str, dict[str, float]]:
    wb = openpyxl.load_workbook(str(path), data_only=True, read_only=True)
    out: dict[str, dict[str, float]] = {}
    for sn in ("รย.1", "รย.2"):
        if sn not in wb.sheetnames:
            continue
        ws = wb[sn]
        sheet_data: dict[str, float] = {}
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i < 5:
                continue
            raw_brand = row[1] if len(row) > 1 else None
            if is_filter_row(raw_brand):
                continue
            if len(row) < 3 or row[2] is None:
                continue
            try:
                count = float(row[2])
            except (ValueError, TypeError):
                continue
            brand = normalize_brand(raw_brand)
            if brand is None:
                continue
            sheet_data[brand] = sheet_data.get(brand, 0.0) + count
        out[sn] = sheet_data
    wb.close()
    return out


def read_file(path: Path) -> dict[str, float]:
    """Merge รย.1 + รย.2 into a single {brand: cumulative}."""
    raw = read_xls(path) if path.suffix.lower() == ".xls" else read_xlsx(path)
    merged: dict[str, float] = defaultdict(float)
    for sheet in raw.values():
        for brand, count in sheet.items():
            merged[brand] += count
    return dict(merged)


def main() -> int:
    files = discover_files()
    if not files:
        print(f"ERROR: no RegisteredBrand_*_Car.xls(x) files found in {DATA_DIR}", file=sys.stderr)
        return 1

    print(f"Discovered {len(files)} files. First: {files[0][2].name}  Last: {files[-1][2].name}")

    # cumulative[(year, month)] = {brand: cumulative_count}
    cumulative: dict[tuple[int, int], dict[str, float]] = {}
    all_brands: set[str] = set()
    suspicious_thai: set[str] = set()

    for year, month, path in files:
        snap = read_file(path)
        cumulative[(year, month)] = snap
        for b in snap:
            all_brands.add(b)
            if has_thai(b):
                suspicious_thai.add(b)
        print(f"  parsed {path.name:48s} -> {len(snap)} brands, total = {int(sum(snap.values())):,}")

    if suspicious_thai:
        print("\nUnmapped Thai brand names (review needed):")
        for b in sorted(suspicious_thai):
            print(f"  {b!r}")
    else:
        print("\nNo unmapped Thai brand names.")

    # ---- compute monthly deltas (Jan 2024 onwards) ----
    monthly_periods = sorted(p for p in cumulative if p >= (2024, 1))
    anomalies = 0
    monthly_records: list[dict] = []
    prev_key = (2023, 12)
    if prev_key not in cumulative:
        print(f"ERROR: missing baseline {prev_key} for monthly deltas", file=sys.stderr)
        return 1

    for key in monthly_periods:
        curr = cumulative[key]
        prev = cumulative.get(prev_key, {})
        year, month = key
        for brand in sorted(set(curr) | set(prev)):
            curr_v = curr.get(brand, 0.0)
            prev_v = prev.get(brand, 0.0)
            delta = curr_v - prev_v
            if delta < 0:
                anomalies += 1
                print(f"WARN negative delta: brand={brand} period={year}-{month:02d} prev={int(prev_v)} curr={int(curr_v)} delta={int(delta)}")
            if delta == 0 and brand not in curr:
                continue
            monthly_records.append({
                "period": "monthly",
                "brand": brand,
                "year": year,
                "month": month,
                "new_registrations": int(round(delta)),
            })
        prev_key = key

    # ---- compute yearly deltas (2020 .. latest year) ----
    yearly_records: list[dict] = []
    years_with_dec = sorted({y for (y, m) in cumulative if m == 12})
    latest_period = max(cumulative.keys())
    latest_year = latest_period[0]

    for y in range(2020, latest_year + 1):
        prev_dec = (y - 1, 12)
        if prev_dec not in cumulative:
            continue
        if y < latest_year:
            curr_key = (y, 12)
            if curr_key not in cumulative:
                continue
            partial = False
            label = str(y)
        else:
            # latest year — use latest available month
            curr_key = latest_period
            partial = curr_key != (y, 12)
            label = f"{y} YTD" if partial else str(y)
        prev = cumulative[prev_dec]
        curr = cumulative[curr_key]
        for brand in sorted(set(curr) | set(prev)):
            curr_v = curr.get(brand, 0.0)
            prev_v = prev.get(brand, 0.0)
            delta = curr_v - prev_v
            if delta < 0:
                anomalies += 1
                print(f"WARN negative delta: brand={brand} period={label} prev={int(prev_v)} curr={int(curr_v)} delta={int(delta)}")
            if delta == 0 and brand not in curr:
                continue
            rec = {
                "period": "yearly",
                "brand": brand,
                "year": y,
                "new_registrations": int(round(delta)),
            }
            if partial:
                rec["partial"] = True
                rec["label"] = label
            yearly_records.append(rec)

    records = monthly_records + yearly_records

    # ---- write NDJSON ----
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ndjson_path = OUT_DIR / "data.ndjson"
    with ndjson_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")

    # Build the same array as a JS literal for embedding
    js_array = "[\n" + ",\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n]"

    html = HTML_TEMPLATE.replace(TEMPLATE_TOKEN, js_array)
    (OUT_DIR / "index.html").write_text(html, encoding="utf-8")

    # ---- summary ----
    monthly_keys = sorted({(r["year"], r["month"]) for r in monthly_records})
    yearly_keys = sorted({r["year"] for r in yearly_records})
    print()
    print("=" * 60)
    print(f"Total records: {len(records):,}  (monthly: {len(monthly_records):,}, yearly: {len(yearly_records):,})")
    print(f"Unique brands: {len(all_brands):,}")
    print(f"Monthly range: {monthly_keys[0][0]}-{monthly_keys[0][1]:02d}  ..  {monthly_keys[-1][0]}-{monthly_keys[-1][1]:02d}")
    print(f"Yearly range:  {yearly_keys[0]}  ..  {yearly_keys[-1]}")
    print(f"Anomalies (negative deltas): {anomalies}")
    print(f"Wrote {ndjson_path}")
    print(f"Wrote {OUT_DIR / 'index.html'}")
    return 0


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Thailand Car Registrations</title>
<script src="vendor/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #fafafa; --fg: #222; --muted: #666; --border: #e0e0e0;
    --accent: #1d4ed8; --accent-bg: #eff6ff;
  }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: var(--bg); color: var(--fg); margin: 0; padding: 16px; }
  h1 { font-size: 20px; margin: 0 0 4px 0; }
  .subtitle { color: var(--muted); font-size: 13px; margin-bottom: 16px; }
  .app { display: grid; grid-template-columns: 220px 1fr; gap: 16px; }
  .sidebar { background: white; border: 1px solid var(--border); border-radius: 8px;
             padding: 12px; max-height: 80vh; overflow-y: auto; }
  .sidebar h2 { font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em;
                color: var(--muted); margin: 0 0 8px 0; }
  .btnrow { display: flex; gap: 6px; margin-bottom: 8px; }
  .btnrow button { flex: 1; font-size: 12px; padding: 4px 6px; border: 1px solid var(--border);
                   background: white; border-radius: 4px; cursor: pointer; }
  .btnrow button:hover { background: var(--accent-bg); }
  .brand-list { display: flex; flex-direction: column; gap: 3px; }
  .brand-list label { font-size: 13px; display: flex; align-items: center; gap: 6px;
                      cursor: pointer; padding: 2px 4px; border-radius: 3px; }
  .brand-list label:hover { background: var(--accent-bg); }
  .brand-list .swatch { width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }
  .main { background: white; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
  .tabs { display: flex; gap: 4px; margin-bottom: 12px; border-bottom: 1px solid var(--border); }
  .tabs button { background: none; border: none; padding: 8px 16px; font-size: 14px; cursor: pointer;
                 color: var(--muted); border-bottom: 2px solid transparent; margin-bottom: -1px; }
  .tabs button.active { color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; }
  .chart-wrap { position: relative; height: 70vh; }
  .footer { color: var(--muted); font-size: 11px; margin-top: 8px; }
</style>
</head>
<body>
  <h1>Thailand car registrations (DLT)</h1>
  <div class="subtitle">New registrations derived from cumulative DLT brand snapshots (รย.1 + รย.2). Source: Department of Land Transport.</div>
  <div class="app">
    <aside class="sidebar">
      <h2>Brands</h2>
      <div class="btnrow">
        <button id="selectAll">Select all</button>
        <button id="clearAll">Clear</button>
      </div>
      <div class="brand-list" id="brandList"></div>
    </aside>
    <section class="main">
      <div class="tabs">
        <button id="tabMonthly" class="active">Monthly</button>
        <button id="tabYearly">Yearly</button>
      </div>
      <div class="chart-wrap"><canvas id="chart"></canvas></div>
      <div class="footer" id="footer"></div>
    </section>
  </div>

<script>
const DATA = __DATA_NDJSON__;

// ------- helpers -------
const PALETTE = [
  "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
  "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf",
  "#aec7e8","#ffbb78","#98df8a","#ff9896","#c5b0d5",
  "#c49c94","#f7b6d2","#c7c7c7","#dbdb8d","#9edae5"
];
function brandColor(brand, idx) {
  // stable color by index in sorted-by-volume order
  return PALETTE[idx % PALETTE.length];
}

const monthly = DATA.filter(d => d.period === "monthly");
const yearly  = DATA.filter(d => d.period === "yearly");

// Rank brands by yearly volume (sum of yearly new_registrations, clamped >= 0)
const totalsByBrand = {};
for (const r of yearly) {
  totalsByBrand[r.brand] = (totalsByBrand[r.brand] || 0) + Math.max(0, r.new_registrations);
}
const allBrands = Object.keys(totalsByBrand).sort((a,b) => totalsByBrand[b] - totalsByBrand[a]);
const colorOf = {};
allBrands.forEach((b, i) => { colorOf[b] = PALETTE[i % PALETTE.length]; });

const TOP_N = 10;
const selected = new Set(allBrands.slice(0, TOP_N));

// ------- monthly x-axis -------
const monthKeys = Array.from(new Set(monthly.map(r => r.year * 100 + r.month))).sort((a,b) => a-b);
const monthLabels = monthKeys.map(k => {
  const y = Math.floor(k/100), m = k % 100;
  return `${y}-${String(m).padStart(2,'0')}`;
});

// ------- yearly x-axis -------
const yearKeys = Array.from(new Set(yearly.map(r => r.year))).sort((a,b) => a-b);
const yearLabels = yearKeys.map(y => {
  const partialRec = yearly.find(r => r.year === y && r.partial);
  return partialRec ? partialRec.label : String(y);
});

// ------- index data for quick lookup -------
const monthlyIdx = {};  // brand -> Map(monthKey -> value)
for (const r of monthly) {
  if (!monthlyIdx[r.brand]) monthlyIdx[r.brand] = {};
  monthlyIdx[r.brand][r.year * 100 + r.month] = r.new_registrations;
}
const yearlyIdx = {};
for (const r of yearly) {
  if (!yearlyIdx[r.brand]) yearlyIdx[r.brand] = {};
  yearlyIdx[r.brand][r.year] = r.new_registrations;
}

// ------- chart state -------
let currentTab = "monthly";
let chart = null;

function buildDatasets() {
  const brands = allBrands.filter(b => selected.has(b));
  if (currentTab === "monthly") {
    return brands.map(b => ({
      label: b,
      data: monthKeys.map(k => monthlyIdx[b]?.[k] ?? 0),
      borderColor: colorOf[b],
      backgroundColor: colorOf[b],
      tension: 0.2,
      borderWidth: 2,
      pointRadius: 2,
    }));
  } else {
    return brands.map(b => ({
      label: b,
      data: yearKeys.map(y => yearlyIdx[b]?.[y] ?? 0),
      borderColor: colorOf[b],
      backgroundColor: colorOf[b],
      tension: 0.2,
      borderWidth: 2,
      pointRadius: 3,
    }));
  }
}

function render() {
  if (chart) chart.destroy();
  const ctx = document.getElementById('chart').getContext('2d');
  const isMonthly = currentTab === "monthly";
  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels: isMonthly ? monthLabels : yearLabels,
      datasets: buildDatasets(),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
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
        y: {
          beginAtZero: true,
          ticks: { callback: (v) => v.toLocaleString() },
          title: { display: true, text: "New registrations" },
        },
        x: {
          title: { display: true, text: isMonthly ? "Month" : "Year" },
        },
      },
    },
  });
  document.getElementById('footer').textContent =
    `${selected.size} of ${allBrands.length} brands selected · ` +
    (isMonthly
      ? `${monthLabels.length} months (${monthLabels[0]} → ${monthLabels[monthLabels.length-1]})`
      : `${yearLabels.length} years (${yearLabels[0]} → ${yearLabels[yearLabels.length-1]})`);
}

// ------- UI wiring -------
function renderBrandList() {
  const list = document.getElementById('brandList');
  list.innerHTML = '';
  for (const b of allBrands) {
    const label = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = selected.has(b);
    cb.addEventListener('change', () => {
      if (cb.checked) selected.add(b); else selected.delete(b);
      render();
    });
    const sw = document.createElement('span');
    sw.className = 'swatch';
    sw.style.background = colorOf[b];
    const txt = document.createElement('span');
    txt.textContent = `${b} (${totalsByBrand[b].toLocaleString()})`;
    label.append(cb, sw, txt);
    list.append(label);
  }
}

document.getElementById('selectAll').addEventListener('click', () => {
  allBrands.forEach(b => selected.add(b));
  renderBrandList();
  render();
});
document.getElementById('clearAll').addEventListener('click', () => {
  selected.clear();
  renderBrandList();
  render();
});
document.getElementById('tabMonthly').addEventListener('click', () => {
  currentTab = "monthly";
  document.getElementById('tabMonthly').classList.add('active');
  document.getElementById('tabYearly').classList.remove('active');
  render();
});
document.getElementById('tabYearly').addEventListener('click', () => {
  currentTab = "yearly";
  document.getElementById('tabYearly').classList.add('active');
  document.getElementById('tabMonthly').classList.remove('active');
  render();
});

renderBrandList();
render();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main())
