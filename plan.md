# Thailand DLT Car Registration Visualization App

## Context

The Thai Department of Land Transport publishes cumulative (สะสม) car-registration snapshots per brand. The user has 26 of these in [dlt-app/data/](dlt-app/data/): year-end snapshots Dec 2019–Dec 2025 plus per-month snapshots Jan 2024 through Apr 2026. We need to (a) convert the cumulative series into per-period **new registrations** by computing deltas between consecutive snapshots, (b) merge passenger-car sheets `รย.1` (≤7 seats) and `รย.2` (>7 seats), (c) normalize and clean brand names, and (d) ship a single self-contained `index.html` that visualizes monthly (Jan 2024 → latest) and yearly (2020 → 2026 YTD) new registrations with a brand multi-select filter.

Output deliverables live in [dlt-app/](dlt-app/):

- [dlt-app/process.py](dlt-app/process.py) — ETL: read XLS/XLSX, normalize brands, compute deltas, emit NDJSON, render `index.html` from a template by string-substituting an embedded data array.
- [dlt-app/run.sh](dlt-app/run.sh) — creates `.venv`, installs deps, runs the pipeline, prints the file:// path to open.
- [dlt-app/output/data.ndjson](dlt-app/output/data.ndjson) — generated; one JSON object per line.
- [dlt-app/output/index.html](dlt-app/output/index.html) — generated; self-contained app.

## Data files in scope

Glob `dlt-app/data/RegisteredBrand_*_Car.xls*` — yields 26 files. **Skip** `30Apr2026.xlsx` (different single-sheet layout), `_Trc.xls` (trucks), and `car_YYYY - w.xlsx` (alternate source).

Filename pattern: `RegisteredBrand_<DD><MonAbbr><YY>_Car.<xls|xlsx>` — e.g., `RegisteredBrand_31Jan24_Car.xls`. Parse with regex `^RegisteredBrand_(\d{1,2})([A-Za-z]{3})(\d{2})_Car\.xlsx?$`, then map `MonAbbr→1..12` and `YY→2000+YY`. Sort chronologically by `(year, month)`.

Note `31Jan26` is `.xlsx` (openpyxl), all others are `.xls` (xlrd). Dispatch by extension.

## XLS layout (uniform across files)

Both `รย.1` and `รย.2` sheets share the layout:

- Rows 0–4: title/header rows (Thai + English)
- Row 5+: brand rows — col 0 = rank, col 1 = brand name, **col 2 = "Whole Kingdom" cumulative count** ← the one we want
- Rows near end: `ยี่ห้ออื่นๆ` (Other brands), `ไม่ระบุ` (Not specified), `  รวม` (Total), then a footer string

**Filter out** any row where `brand` (after strip):
- is empty / None
- starts with `รวม` (the totals row)
- equals `ยี่ห้ออื่นๆ` or contains `ยี่ห้ออื่น`
- equals `ไม่ระบุ`
- starts with `กลุ่ม` (the footer)
- col 0 (rank) is non-numeric AND brand isn't a valid one — belt-and-suspenders

## Brand normalization

In practice the data already uses uppercase English. Apply this minimal pipeline:

1. `brand.strip()` then collapse runs of whitespace to single spaces, then `.upper()`.
2. Apply a small alias dict (keep in `process.py` near the top so it's editable):
   - `MERCEDES BENZ` → `MERCEDES-BENZ`
   - `ROLLS ROYCE` → `ROLLS-ROYCE`
   - `LANDROVER` → `LAND ROVER`
   - `M.G.` → `MG`
3. Collect any brand that contains Thai characters AND isn't in the filter list above — print them to stdout under "Unmapped Thai brand names:" for manual review (none expected based on inspection, but defensive).

## Delta computation

Build a long DataFrame keyed by `(brand, year, month)` with `cumulative` value (sum of `รย.1` + `รย.2` for that brand in that file).

**Monthly view** (Jan 2024 → latest, Apr 2026):
- For each (brand, month in 2024-01…latest): `new = cum[m] - cum[prev_m]`
- `prev_m` for Jan 2024 = Dec 2023 (yearly snapshot)
- If a brand is absent from `prev_m`, treat its prior cumulative as 0 (it's a new brand). Document this in code.

**Yearly view** (2020 → 2026 YTD):
- For Y in 2020..2025: `new = cum[Dec Y] - cum[Dec Y-1]`
- For 2026: `new = cum[latest 2026 month] - cum[Dec 2025]`; tag with `label: "2026 YTD"` and `partial: true`

**Anomaly detection**: if any computed delta < 0, print warning to stdout:
```
WARN negative delta: brand=X period=2024-03 prev=12345 curr=12000 delta=-345
```
Still emit the (negative) value; the HTML can show it as-is — surfacing data quirks is better than silently clamping.

## NDJSON output

[dlt-app/output/data.ndjson](dlt-app/output/data.ndjson) — one record per line. Two record shapes in the same file, distinguished by `period`:

```json
{"period":"monthly","brand":"TOYOTA","year":2024,"month":1,"new_registrations":1234}
{"period":"yearly","brand":"TOYOTA","year":2024,"new_registrations":56789}
{"period":"yearly","brand":"TOYOTA","year":2026,"new_registrations":12345,"partial":true,"label":"2026 YTD"}
```

This keeps a single file (matches the spec) while letting the HTML branch by `period`.

Also print a stdout summary: total records emitted, unique brand count, date range, anomalies count.

## HTML application

`process.py` reads a string template (defined inline in `process.py` as a Python triple-quoted string — keeps everything in one Python file as requested, no separate template file to manage) and substitutes a single token `__DATA_NDJSON__` with the literal NDJSON content embedded as a JS array literal.

Inline JS:
- Parse `const DATA = [...]` at load.
- Compute top-10 brands by sum of `new_registrations` across **all** records (monthly + yearly, deduped at the year-end level so as not to double-count) — simpler: rank by sum of yearly records only.
- Render brand checkbox list, top 10 pre-checked. Two buttons: "Select All" / "Clear".
- Two tab buttons: "Monthly" / "Yearly".
- Monthly tab: Chart.js line chart, x-axis = months Jan 2024 → Apr 2026, one dataset per selected brand.
- Yearly tab: Chart.js bar chart, x-axis = 2020, 2021, …, 2025, "2026 YTD" (italicized via tick callback), grouped bars per selected brand.
- Color palette: a stable 20-color palette indexed by brand-name hash (consistent colors across re-renders).
- Re-render charts on filter change.

Chart.js is **vendored locally** at [dlt-app/output/vendor/chart.umd.min.js](dlt-app/output/vendor/chart.umd.min.js) and referenced via a relative `<script src="vendor/chart.umd.min.js">`. The HTML works fully offline once generated.

A separate helper script [dlt-app/update_chartjs.sh](dlt-app/update_chartjs.sh) fetches the latest Chart.js UMD build:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p output/vendor
# Query npm registry for the latest version, then download the UMD bundle from jsDelivr
VERSION=$(curl -fsSL https://registry.npmjs.org/chart.js/latest | python3 -c "import sys,json;print(json.load(sys.stdin)['version'])")
echo "Downloading Chart.js v${VERSION}"
curl -fsSL "https://cdn.jsdelivr.net/npm/chart.js@${VERSION}/dist/chart.umd.min.js" -o output/vendor/chart.umd.min.js
echo "${VERSION}" > output/vendor/chart.version.txt
echo "Saved output/vendor/chart.umd.min.js (v${VERSION})"
```

`run.sh` invokes `update_chartjs.sh` once if `output/vendor/chart.umd.min.js` is missing (so first-run works end-to-end), but the user can re-run `update_chartjs.sh` at any time to upgrade.

## run.sh

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet pandas xlrd openpyxl
if [ ! -f output/vendor/chart.umd.min.js ]; then
  ./update_chartjs.sh
fi
.venv/bin/python process.py
echo "Open: file://$(pwd)/output/index.html"
```

Make `run.sh` and `update_chartjs.sh` executable; user runs `./run.sh` from `dlt-app/`. To upgrade Chart.js later, run `./update_chartjs.sh`.

## Critical files to be created

- [dlt-app/process.py](dlt-app/process.py) — single-file ETL + HTML render
- [dlt-app/run.sh](dlt-app/run.sh) — bootstrap + run script
- [dlt-app/update_chartjs.sh](dlt-app/update_chartjs.sh) — fetches the latest Chart.js UMD bundle into `output/vendor/`

Generated (gitignored category):
- [dlt-app/output/data.ndjson](dlt-app/output/data.ndjson)
- [dlt-app/output/index.html](dlt-app/output/index.html)
- [dlt-app/output/vendor/chart.umd.min.js](dlt-app/output/vendor/chart.umd.min.js)
- [dlt-app/output/vendor/chart.version.txt](dlt-app/output/vendor/chart.version.txt)
- [dlt-app/.venv/](dlt-app/.venv/)

## Verification

1. **Run end-to-end**: `cd dlt-app && ./run.sh` — should complete without traceback and print the file:// path.
2. **Sanity-check NDJSON**:
   - `wc -l output/data.ndjson` ≈ (brands × months_2024+monthly_count) + (brands × 7 years)
   - `grep '"brand":"TOYOTA"' output/data.ndjson | grep '"year":2024' | grep '"period":"yearly"'` — value should land in the 200k–400k range (Toyota's annual Thai car registrations), NOT in the millions (which would indicate raw cumulative leaked through).
   - `grep '"period":"monthly"' output/data.ndjson | grep '"year":2024' | grep '"month":1'` — TOYOTA Jan 2024 should be in the tens of thousands.
3. **Anomaly warnings**: confirm the script prints any negative deltas (or "no anomalies found"). Spot-check at least one warning if printed.
4. **Open `output/index.html` in Brave/Chrome**:
   - Both tabs render.
   - Top 10 brands pre-checked (TOYOTA, HONDA, ISUZU, MITSUBISHI, NISSAN, MAZDA, FORD, MG, SUZUKI, BYD or similar).
   - Toggling a checkbox redraws the chart.
   - "Select All" checks every box; "Clear" unchecks. Chart updates accordingly.
   - Monthly chart x-axis spans Jan 2024 → Apr 2026; Yearly chart x-axis is `2020 2021 2022 2023 2024 2025 "2026 YTD"`.
   - No JS errors in console.
5. **No-server check**: copy the entire `output/` directory to a temp location — `index.html` should render with full functionality (data is embedded; `vendor/chart.umd.min.js` is a relative load and travels with the folder). True offline: disconnect network, refresh — chart still renders.
6. **Chart.js updater check**: run `./update_chartjs.sh`; verify `output/vendor/chart.version.txt` contains a current version string and `chart.umd.min.js` is non-empty.
