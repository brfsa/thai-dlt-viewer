#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p output/vendor
VERSION=$(curl -fsSL https://registry.npmjs.org/chart.js/latest \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['version'])")
echo "Downloading Chart.js v${VERSION}"
curl -fsSL "https://cdn.jsdelivr.net/npm/chart.js@${VERSION}/dist/chart.umd.min.js" \
  -o output/vendor/chart.umd.min.js
echo "${VERSION}" > output/vendor/chart.version.txt
SIZE=$(wc -c < output/vendor/chart.umd.min.js | tr -d ' ')
echo "Saved output/vendor/chart.umd.min.js (v${VERSION}, ${SIZE} bytes)"
