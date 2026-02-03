"""E2E tests for BackfillEngine."""

import sqlite3
from pathlib import Path

import pytest

from igh_data_transform.temporal_backfill import BackfillEngine


class TestBackfillEngine:
    """E2E tests for temporal backfill transformation."""

    def test_backfill_creates_scd2_versions_with_temporal_boundaries(
        self, raw_db: Path, column_report: Path, output_db_path: Path
    ):
        """Verify entity with changing temporal values produces correct SCD2 versions."""
        # Setup: Insert entity with different values across years
        conn = sqlite3.connect(str(raw_db))
        conn.execute("""
            INSERT INTO vin_candidates (row_id, vin_id, vin_name, new_funders2021, new_funders2024, new_funders)
            VALUES (1, 'VIN-001', 'Test Product', 'Funder A', 'Funder B', 'Funder C')
        """)
        conn.commit()
        conn.close()

        # Run backfill
        engine = BackfillEngine(
            str(raw_db), str(column_report), str(output_db_path), current_year=2025
        )
        engine.run()

        # Verify output
        conn = sqlite3.connect(str(output_db_path))
        conn.row_factory = sqlite3.Row

        # Check table exists
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='vin_candidates'"
        ).fetchall()
        assert len(tables) == 1

        # Check versions for VIN-001
        rows = conn.execute(
            "SELECT * FROM vin_candidates WHERE vin_id = 'VIN-001' ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 3

        # Version 1: 2021-2024
        assert rows[0]["valid_from"] == "2021-01-01T00:00:00Z"
        assert rows[0]["valid_to"] == "2024-01-01T00:00:00Z"
        assert rows[0]["new_funders"] == "Funder A"

        # Version 2: 2024-2025
        assert rows[1]["valid_from"] == "2024-01-01T00:00:00Z"
        assert rows[1]["valid_to"] == "2025-01-01T00:00:00Z"
        assert rows[1]["new_funders"] == "Funder B"

        # Version 3: 2025-current
        assert rows[2]["valid_from"] == "2025-01-01T00:00:00Z"
        assert rows[2]["valid_to"] is None
        assert rows[2]["new_funders"] == "Funder C"

        # Check schema: year-suffixed columns removed, base column exists
        columns = [
            row[1]
            for row in conn.execute("PRAGMA table_info(vin_candidates)").fetchall()
        ]
        assert "new_funders" in columns
        assert "new_funders2021" not in columns
        assert "new_funders2024" not in columns

        conn.close()

    def test_backfill_copies_non_temporal_tables_unchanged(
        self, raw_db: Path, column_report: Path, output_db_path: Path
    ):
        """Verify non-temporal tables are copied as-is."""
        # Setup: Insert at least one entity in temporal table (required by engine)
        conn = sqlite3.connect(str(raw_db))
        conn.execute("""
            INSERT INTO vin_candidates (row_id, vin_id, vin_name, new_funders)
            VALUES (1, 'VIN-001', 'Test', 'Some Funder')
        """)
        conn.commit()
        conn.close()

        # Run backfill
        engine = BackfillEngine(
            str(raw_db), str(column_report), str(output_db_path), current_year=2025
        )
        engine.run()

        # Verify vin_metadata in output
        conn = sqlite3.connect(str(output_db_path))
        conn.row_factory = sqlite3.Row

        # Check table exists
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='vin_metadata'"
        ).fetchall()
        assert len(tables) == 1

        # Check schema matches
        columns = [
            row[1]
            for row in conn.execute("PRAGMA table_info(vin_metadata)").fetchall()
        ]
        assert columns == ["id", "key", "value"]

        # Check data preserved
        rows = conn.execute("SELECT * FROM vin_metadata ORDER BY id").fetchall()
        assert len(rows) == 2
        assert dict(rows[0]) == {"id": 1, "key": "version", "value": "1.0"}
        assert dict(rows[1]) == {"id": 2, "key": "source", "value": "dataverse"}

        conn.close()

    def test_backfill_handles_empty_temporal_table(
        self, raw_db: Path, column_report: Path, output_db_path: Path
    ):
        """Verify backfill succeeds when temporal table has no rows."""
        # raw_db fixture has empty vin_candidates table
        engine = BackfillEngine(
            str(raw_db), str(column_report), str(output_db_path), current_year=2025
        )
        engine.run()

        # Verify output table exists but is empty
        conn = sqlite3.connect(str(output_db_path))
        count = conn.execute("SELECT COUNT(*) FROM vin_candidates").fetchone()[0]
        assert count == 0
        conn.close()
