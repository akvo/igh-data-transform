"""Session-scoped fixtures for e2e tests using a real Bronze database."""

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

from igh_data_transform.transformations.bronze_to_silver import bronze_to_silver

# Load .env so credentials are available in os.environ
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_dotenv_path = _PROJECT_ROOT / ".env"
if _dotenv_path.exists():
    load_dotenv(_dotenv_path)

CORE_TABLES = ["vin_candidates", "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities"]

REQUIRED_ENV_VARS = [
    "DATAVERSE_API_URL",
    "DATAVERSE_CLIENT_ID",
    "DATAVERSE_CLIENT_SECRET",
    "DATAVERSE_SCOPE",
]


def _has_core_tables(db_path: Path) -> bool:
    """Check if the Bronze DB contains the 4 core tables."""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()
        return all(t in tables for t in CORE_TABLES)
    except Exception:
        return False


def _has_credentials() -> bool:
    """Check if all Dataverse credentials are available."""
    return all(os.environ.get(var) for var in REQUIRED_ENV_VARS)


@pytest.fixture(scope="session")
def bronze_db_path() -> Path:
    """Provide a path to a real Bronze database.

    Resolution order:
    1. Use E2E_BRONZE_DB_PATH env var if set
    2. Use cached tests/data/bronze.db if it exists and has core tables
    3. Run sync-dataverse to create one (requires Dataverse credentials)
    4. Skip if no credentials available
    """
    custom_path = os.environ.get("E2E_BRONZE_DB_PATH")
    if custom_path:
        path = Path(custom_path)
        if path.exists() and _has_core_tables(path):
            return path
        pytest.skip(
            f"E2E_BRONZE_DB_PATH={custom_path} does not exist or is missing core tables"
        )

    # Default cached location
    project_root = Path(__file__).resolve().parent.parent.parent
    cached_path = project_root / "tests" / "data" / "bronze.db"

    if cached_path.exists() and _has_core_tables(cached_path):
        return cached_path

    # Try to sync from Dataverse
    if not _has_credentials():
        pytest.skip(
            "No Bronze DB available and Dataverse credentials not set. "
            "Either set E2E_BRONZE_DB_PATH to a pre-existing Bronze DB, "
            "or set DATAVERSE_API_URL, DATAVERSE_CLIENT_ID, "
            "DATAVERSE_CLIENT_SECRET, and DATAVERSE_SCOPE."
        )

    # Create the data directory and run sync
    cached_path.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["SQLITE_DB_PATH"] = str(cached_path)

    result = subprocess.run(
        [sys.executable, "-m", "igh_data_sync.scripts.sync"],
        env=env,
        capture_output=True,
        text=True,
        timeout=1200,
    )

    if result.returncode != 0:
        pytest.fail(
            f"sync-dataverse failed (exit code {result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    if not cached_path.exists() or not _has_core_tables(cached_path):
        pytest.fail(
            "sync-dataverse completed but Bronze DB is missing or incomplete"
        )

    return cached_path


@pytest.fixture(scope="session")
def silver_db_path(bronze_db_path, tmp_path_factory) -> Path:
    """Run bronze_to_silver once and return the Silver DB path."""
    silver_dir = tmp_path_factory.mktemp("silver")
    silver_path = silver_dir / "silver.db"

    success = bronze_to_silver(str(bronze_db_path), str(silver_path))

    if not success:
        pytest.fail("bronze_to_silver() returned False during e2e setup")

    return silver_path


@pytest.fixture(scope="session")
def silver_conn(silver_db_path) -> sqlite3.Connection:
    """Provide a shared read-only connection to the Silver database."""
    conn = sqlite3.connect(str(silver_db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
