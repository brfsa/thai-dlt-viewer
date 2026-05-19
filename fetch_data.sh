#!/usr/bin/env bash
# Best-effort downloader for DLT new-car-registration Excel exports.
#
# Usage:   ./fetch_data.sh <ID>           e.g. ./fetch_data.sh 10663
#          ./fetch_data.sh 10662 10663    # multiple
#
# Source URL pattern (unpublished, may change without notice):
#   https://web.dlt.go.th/statistics/load_file_select_new_car.php?t=2&tmp=<rand>&data_file=<ID>
#
# DLT may rotate the file IDs when new data is published. If this script
# starts returning 404s or empty payloads, visit
#   https://web.dlt.go.th/statistics/index.php
# manually, look for "สถิติการจดทะเบียนรถใหม่ ตามกฎหมายว่าด้วยรถยนต์",
# inspect the network tab while clicking the download links, and update
# the IDs you pass to this script.
set -euo pipefail
cd "$(dirname "$0")"

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <ID> [<ID> ...]" >&2
  echo "  e.g. $0 10662 10663" >&2
  exit 2
fi

mkdir -p data/cars-data

for ID in "$@"; do
  TMP="$(awk 'BEGIN{srand(); print rand()}')"
  URL="https://web.dlt.go.th/statistics/load_file_select_new_car.php?t=2&tmp=${TMP}&data_file=${ID}"
  OUT="data/cars-data/${ID}.xlsx"
  echo "Fetching ${ID} ..."
  if curl -fsSL "${URL}" -o "${OUT}.tmp"; then
    # Reject obvious HTML error pages (DLT sometimes returns a 200 + login page)
    if head -c 4 "${OUT}.tmp" | grep -q '^PK'; then
      mv "${OUT}.tmp" "${OUT}"
      ls -lh "${OUT}"
    else
      echo "  ERROR: ${ID} did not return an xlsx (first bytes were not 'PK..')." >&2
      head -c 200 "${OUT}.tmp" >&2
      echo >&2
      rm -f "${OUT}.tmp"
      exit 1
    fi
  else
    echo "  curl failed for ${ID}." >&2
    rm -f "${OUT}.tmp"
    exit 1
  fi
done
