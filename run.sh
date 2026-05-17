#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  echo "Creating virtualenv at .venv ..."
  python3 -m venv .venv
fi

.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet pandas xlrd openpyxl

if [ ! -f output/vendor/chart.umd.min.js ]; then
  ./update_chartjs.sh
fi

.venv/bin/python process.py

echo
echo "Open: file://$(pwd)/output/index.html"
