#!/bin/bash
set -euo pipefail

REPO_DIR="/Users/uc_bilin/.openclaw/workspace/statichtml"
cd "$REPO_DIR"

/usr/bin/python3 scripts/generate_prediction_markets_today.py

/usr/bin/git add data/prediction-markets-today.json prediction-markets-today.json prediction-markets-today.html scripts/generate_prediction_markets_today.py scripts/update_prediction_markets_today.sh

if ! /usr/bin/git diff --cached --quiet; then
  /usr/bin/git commit -m "chore: update prediction markets today $(date '+%Y-%m-%d %H:%M:%S %Z')"
  /usr/bin/git push origin main
fi
