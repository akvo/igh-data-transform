"""Shared fixtures for temporal backfill E2E tests."""

import json
import sqlite3
from pathlib import Path

import pytest


@pytest.fixture
def raw_db(tmp_path: Path) -> Path:
    """Create a minimal SQLite database with temporal and non-temporal tables."""
    db_path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(db_path))

    # Temporal table: vin_candidates
    conn.execute("""
        CREATE TABLE vin_candidates (
            row_id INTEGER PRIMARY KEY,
            vin_id TEXT NOT NULL,
            vin_name TEXT,
            new_funders2021 TEXT,
            new_funders2024 TEXT,
            new_funders TEXT,
            valid_from TEXT,
            valid_to TEXT
        )
    """)

    # Non-temporal table: vin_metadata
    conn.execute("""
        CREATE TABLE vin_metadata (
            id INTEGER PRIMARY KEY,
            key TEXT NOT NULL,
            value TEXT
        )
    """)
    conn.execute("""
        INSERT INTO vin_metadata (id, key, value)
        VALUES
            (1, 'version', '1.0'),
            (2, 'source', 'dataverse')
    """)

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def column_report(tmp_path: Path) -> Path:
    """Create the temporal columns report JSON file."""
    report = {
        "summary": {"total_temporal_groups": 1, "total_temporal_columns": 3},
        "tables": {
            "vin_candidates": {
                "total_columns": 8,
                "temporal_columns_count": 3,
                "temporal_groups": {
                    "new_funders": [
                        {"column": "new_funders2021", "year": 2021},
                        {"column": "new_funders2024", "year": 2024},
                        {"column": "new_funders", "year": 9999},
                    ]
                },
            }
        },
    }

    report_path = tmp_path / "temporal_columns_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    return report_path


@pytest.fixture
def output_db_path(tmp_path: Path) -> Path:
    """Return the path for the output database."""
    return tmp_path / "output.db"
