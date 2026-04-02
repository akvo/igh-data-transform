#!/usr/bin/env bash
# Sync Dataverse data then run the full ETL pipeline:
#   Sync -> Bronze -> Silver -> Star Schema -> Backend
#
# Usage:
#   ./sync-and-run-etl.sh                          # sync + full pipeline
#   ./sync-and-run-etl.sh --skip-sync              # skip sync, use existing bronze DB
#   ./sync-and-run-etl.sh --fresh                  # delete existing bronze DB and sync fresh
#   ./sync-and-run-etl.sh --env-file /path/to/.env # custom .env for sync

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
BRONZE_DB="$DATA_DIR/dataverse_complete_raw.db"
SILVER_DB="$DATA_DIR/silver.db"
GOLD_DB="$DATA_DIR/star_schema.db"
BACKEND_DIR="$SCRIPT_DIR/../backend"

SKIP_SYNC=false
FRESH=false
ENV_FILE_ARG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-sync)
            SKIP_SYNC=true
            shift
            ;;
        --fresh)
            FRESH=true
            shift
            ;;
        --env-file)
            ENV_FILE_ARG="--env-file $2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--skip-sync] [--fresh] [--env-file /path/to/.env]"
            exit 1
            ;;
    esac
done

# Step 0: Sync Dataverse
if [ "$SKIP_SYNC" = false ]; then
    if [ "$FRESH" = true ] && [ -f "$BRONZE_DB" ]; then
        echo "  --fresh: Removing existing bronze DB"
        rm "$BRONZE_DB"
    fi
    echo "=== Step 1/4: Sync Dataverse ==="
    echo "  Output: $BRONZE_DB"
    cd "$SCRIPT_DIR"
    # Intentional word splitting: ENV_FILE_ARG may be empty or "--env-file path"
    # shellcheck disable=SC2086
    SQLITE_DB_PATH="$BRONZE_DB" uv run sync-dataverse $ENV_FILE_ARG
    echo ""
fi

# Step 1: Bronze -> Silver
if [ "$SKIP_SYNC" = true ]; then
    echo "=== Step 1/3: Bronze -> Silver ==="
else
    echo "=== Step 2/4: Bronze -> Silver ==="
fi

if [ ! -f "$BRONZE_DB" ]; then
    echo "Error: Bronze DB not found at $BRONZE_DB"
    exit 1
fi

echo "  Bronze: $BRONZE_DB"
echo "  Silver: $SILVER_DB"
cd "$SCRIPT_DIR"
uv run igh-transform bronze-to-silver --bronze-db "$BRONZE_DB" --silver-db "$SILVER_DB"

# Step 2: Silver -> Star Schema
echo ""
if [ "$SKIP_SYNC" = true ]; then
    echo "=== Step 2/3: Silver -> Star Schema ==="
else
    echo "=== Step 3/4: Silver -> Star Schema ==="
fi
echo "  Source: $SILVER_DB"
echo "  Output: $GOLD_DB"
uv run igh-transform silver-to-gold --silver-db "$SILVER_DB" --gold-db "$GOLD_DB"

# Step 3: Copy to backend
echo ""
if [ "$SKIP_SYNC" = true ]; then
    echo "=== Step 3/3: Copy to backend ==="
else
    echo "=== Step 4/4: Copy to backend ==="
fi
cp "$GOLD_DB" "$BACKEND_DIR/star_schema.db"
cp "$GOLD_DB" "$BACKEND_DIR/tests/star_schema.db"
echo "  Copied to $BACKEND_DIR/star_schema.db"
echo "  Copied to $BACKEND_DIR/tests/star_schema.db"

echo ""
echo "=== ETL pipeline complete ==="
