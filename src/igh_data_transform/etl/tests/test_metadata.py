"""Tests for ETL metadata extraction and loading."""

import sqlite3

import pytest

from src.extractor import Extractor
from src.loader import Loader


class TestGetLastSyncDate:
    """Test Extractor.get_last_sync_date()."""

    @pytest.fixture
    def db_with_sync_log(self, tmp_path):
        """Create a temporary DB with _sync_log table."""
        db_path = tmp_path / "source.db"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE _sync_log (
                id INTEGER PRIMARY KEY,
                status TEXT,
                records_added INTEGER,
                records_updated INTEGER,
                end_time TEXT
            )
        """)
        conn.commit()
        return db_path, conn

    def test_returns_latest_completed(self, db_with_sync_log):
        """Returns the most recent completed sync with changes."""
        db_path, conn = db_with_sync_log
        conn.executemany(
            "INSERT INTO _sync_log (status, records_added, records_updated, end_time) VALUES (?, ?, ?, ?)",
            [
                ("completed", 10, 0, "2026-01-15T10:00:00"),
                ("completed", 5, 3, "2026-02-04T12:00:00"),
                ("completed", 1, 0, "2026-01-20T08:00:00"),
            ],
        )
        conn.commit()
        conn.close()

        extractor = Extractor(db_path)
        extractor.connect()
        assert extractor.get_last_sync_date() == "2026-02-04T12:00:00"
        extractor.close()

    def test_ignores_failed_and_zero_change_syncs(self, db_with_sync_log):
        """Ignores syncs that failed or had zero changes."""
        db_path, conn = db_with_sync_log
        conn.executemany(
            "INSERT INTO _sync_log (status, records_added, records_updated, end_time) VALUES (?, ?, ?, ?)",
            [
                ("completed", 5, 0, "2026-01-10T10:00:00"),
                ("failed", 0, 0, "2026-02-05T12:00:00"),
                ("completed", 0, 0, "2026-02-06T08:00:00"),
            ],
        )
        conn.commit()
        conn.close()

        extractor = Extractor(db_path)
        extractor.connect()
        assert extractor.get_last_sync_date() == "2026-01-10T10:00:00"
        extractor.close()

    def test_returns_none_when_empty(self, db_with_sync_log):
        """Returns None when no qualifying syncs exist."""
        db_path, conn = db_with_sync_log
        conn.close()

        extractor = Extractor(db_path)
        extractor.connect()
        assert extractor.get_last_sync_date() is None
        extractor.close()


class TestWriteMetadata:
    """Test Loader.write_metadata()."""

    @pytest.fixture
    def loader(self, tmp_path):
        """Create a Loader pointing at a temporary DB."""
        db_path = tmp_path / "star_schema.db"
        ldr = Loader(db_path)
        ldr.connect()
        yield ldr
        ldr.close()

    def test_creates_table_and_inserts(self, loader):
        """Creates _etl_metadata table and inserts key-value pairs."""
        loader.write_metadata({"last_sync_date": "2026-02-04T12:00:00"})

        cursor = loader._get_cursor()
        cursor.execute("SELECT key, value FROM _etl_metadata")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "last_sync_date"
        assert rows[0][1] == "2026-02-04T12:00:00"

    def test_overwrites_on_rerun(self, loader):
        """Overwrites existing metadata on subsequent calls."""
        loader.write_metadata({"last_sync_date": "2026-01-01T00:00:00"})
        loader.write_metadata({"last_sync_date": "2026-02-04T12:00:00"})

        cursor = loader._get_cursor()
        cursor.execute("SELECT value FROM _etl_metadata WHERE key = 'last_sync_date'")
        row = cursor.fetchone()
        assert row[0] == "2026-02-04T12:00:00"
