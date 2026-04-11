#!/usr/bin/env bash
# Apply schema + seed demo telemetry so Streamlit has charts to show.
set -euo pipefail
cd "$(dirname "$0")/.."
python scripts/bootstrap_duckdb.py
python scripts/seed_demo_data.py --jobs 400 --days 14
echo ""
echo "Next (from this directory):"
echo "  streamlit run src/dashboards/app.py"
echo "Then open the sidebar → Refresh data if the app was already running."
