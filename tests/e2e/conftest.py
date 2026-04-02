"""Session-scoped fixtures for e2e tests using a real Bronze database."""

import os
import sqlite3
from pathlib import Path

import pytest
from igh_data_transform.transformations.bronze_to_silver import bronze_to_silver
from igh_data_transform.transformations.silver_to_gold import silver_to_gold

CORE_TABLES = ["vin_candidates", "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities"]


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


@pytest.fixture(scope="session")
def bronze_db_path() -> Path:
    """Provide a path to a real Bronze database.

    The path must be supplied via the E2E_BRONZE_DB_PATH environment variable.
    """
    custom_path = os.environ.get("E2E_BRONZE_DB_PATH")
    if not custom_path:
        pytest.skip(
            "E2E_BRONZE_DB_PATH not set. "
            "Point it at a fully populated Bronze DB to run e2e tests."
        )

    path = Path(custom_path)
    if not path.exists() or not _has_core_tables(path):
        pytest.skip(
            f"E2E_BRONZE_DB_PATH={custom_path} does not exist or is missing core tables"
        )

    return path


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


@pytest.fixture(scope="session")
def gold_db_path(silver_db_path, tmp_path_factory) -> Path:
    """Run silver_to_gold once and return the Gold DB path."""
    gold_dir = tmp_path_factory.mktemp("gold")
    gold_path = gold_dir / "star_schema.db"

    success = silver_to_gold(str(silver_db_path), str(gold_path))

    if not success:
        pytest.fail("silver_to_gold() returned False during e2e setup")

    return gold_path


@pytest.fixture(scope="session")
def gold_conn(gold_db_path) -> sqlite3.Connection:
    """Provide a shared read-only connection to the Gold database."""
    conn = sqlite3.connect(str(gold_db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
