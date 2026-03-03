#!/usr/bin/env bash
# Run the full ETL pipeline: Bronze -> Silver -> Star Schema -> Backend
#
# Usage:
#   ./run-etl.sh                        # uses default paths
#   ./run-etl.sh /path/to/bronze.db     # custom bronze DB path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BRONZE_DB="${1:-$SCRIPT_DIR/dataverse_complete_raw.db}"
SILVER_DB="$SCRIPT_DIR/silver_test.db"
ETL_DIR="$SCRIPT_DIR/src/igh_data_transform/etl"
STAR_SCHEMA="$ETL_DIR/output/star_schema.db"
BACKEND_DIR="$SCRIPT_DIR/../backend"

if [ ! -f "$BRONZE_DB" ]; then
    echo "Error: Bronze DB not found at $BRONZE_DB"
    exit 1
fi

echo "=== Step 1/3: Bronze -> Silver ==="
echo "  Bronze: $BRONZE_DB"
echo "  Silver: $SILVER_DB"
cd "$SCRIPT_DIR"
uv run igh-transform bronze-to-silver --bronze-db "$BRONZE_DB" --silver-db "$SILVER_DB"

echo ""
echo "=== Step 2/3: Silver -> Star Schema ==="
echo "  Source: $SILVER_DB"
echo "  Output: $STAR_SCHEMA"
cd "$ETL_DIR"
uv run python -m src.main --source "$SILVER_DB" --output "$STAR_SCHEMA"

echo ""
echo "=== Step 3/3: Copy to backend ==="
cp "$STAR_SCHEMA" "$BACKEND_DIR/star_schema.db"
cp "$STAR_SCHEMA" "$SCRIPT_DIR/star_schema.db"
echo "  Copied to $BACKEND_DIR/star_schema.db"
echo "  Copied to $SCRIPT_DIR/star_schema.db"

echo ""
echo "=== ETL pipeline complete ==="
