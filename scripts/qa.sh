#!/usr/bin/env bash
# QA checks: linting, formatting, and tests.
# The GitHub Actions workflow calls each step individually so they appear
# as separate checks, while `bash scripts/qa.sh` runs everything locally.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DEFAULT_BRONZE_DB="$REPO_DIR/data/dataverse_complete_raw.db"

# Auto-enable e2e tests when the default Bronze DB exists and
# E2E_BRONZE_DB_PATH hasn't been explicitly set.
if [ -z "${E2E_BRONZE_DB_PATH:-}" ] && [ -f "$DEFAULT_BRONZE_DB" ]; then
    export E2E_BRONZE_DB_PATH="$DEFAULT_BRONZE_DB"
fi

pytest_e2e_flags() {
    if [ -n "${E2E_BRONZE_DB_PATH:-}" ]; then
        echo "--all"
    fi
}

step_lint() {
    echo "==> Ruff lint"
    uv run ruff check src/ tests/
}

step_format() {
    echo "==> Ruff format check"
    uv run ruff format --check src/ tests/
}

step_test() {
    echo "==> Tests"
    # shellcheck disable=SC2046
    uv run pytest --cov=igh_data_transform --cov-report=term-missing \
        $(pytest_e2e_flags) "$@"
}

case "${1:-all}" in
    lint)    step_lint ;;
    format)  step_format ;;
    test)    shift; step_test "$@" ;;
    all)     shift 2>/dev/null || true; step_lint && step_format && step_test "$@" ;;
    *)       echo "Usage: $0 {lint|format|test|all} [extra pytest args...]"; exit 1 ;;
esac
