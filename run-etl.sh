#!/usr/bin/env bash
# Run the full ETL pipeline: Bronze -> Silver -> Star Schema -> Backend
#
# Usage:
#   ./run-etl.sh                        # uses default paths
#   ./run-etl.sh /path/to/bronze.db     # custom bronze DB path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
BRONZE_DB="${1:-$DATA_DIR/dataverse_complete_raw.db}"
SILVER_DB="$DATA_DIR/silver.db"
GOLD_DB="$DATA_DIR/star_schema.db"
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
echo "  Output: $GOLD_DB"
uv run igh-transform silver-to-gold --silver-db "$SILVER_DB" --gold-db "$GOLD_DB"

echo ""
echo "=== Step 3/3: Copy to backend ==="
cp "$GOLD_DB" "$BACKEND_DIR/star_schema.db"
cp "$GOLD_DB" "$BACKEND_DIR/tests/star_schema.db"
echo "  Copied to $BACKEND_DIR/star_schema.db"
echo "  Copied to $BACKEND_DIR/tests/star_schema.db"

echo ""
echo "=== ETL pipeline complete ==="
