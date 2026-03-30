"""Tests for silver_to_gold Extractor."""

import sqlite3

import pytest

from igh_data_transform.transformations.silver_to_gold.core.extractor import Extractor


@pytest.fixture
def source_db(tmp_path):
    """Create a temporary SQLite DB with test data."""
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE vin_candidates (candidateid TEXT, new_currentrdstage TEXT)")
    conn.execute("INSERT INTO vin_candidates VALUES ('c1', 'Phase I'), ('c2', 'Phase II'), ('c2', 'Phase II')")
    conn.execute("CREATE TABLE _optionset_teststatus (code INTEGER, label TEXT)")
    conn.execute("INSERT INTO _optionset_teststatus VALUES (1, 'Active'), (2, 'Inactive')")
    conn.execute("CREATE TABLE _sync_log (end_time TEXT, status TEXT, records_added INTEGER, records_updated INTEGER)")
    conn.execute("INSERT INTO _sync_log VALUES ('2025-03-01T10:00:00', 'completed', 50, 50)")
    conn.execute("INSERT INTO _sync_log VALUES ('2025-03-02T10:00:00', 'completed', 0, 0)")
    conn.commit()
    conn.close()
    return db_path


class TestExtractorContextManager:
    def test_enter_exit(self, source_db):
        with Extractor(source_db) as ext:
            assert ext is not None
        # Connection should be closed after exit

    def test_invalid_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Extractor(tmp_path / "nonexistent.db")


class TestBuildOptionsetCache:
    def test_caches_optionset_tables(self, source_db):
        with Extractor(source_db) as ext:
            ext.build_optionset_cache()
            assert ext.lookup_optionset("teststatus", 1) == "Active"
            assert ext.lookup_optionset("teststatus", 2) == "Inactive"

    def test_missing_code_returns_none(self, source_db):
        with Extractor(source_db) as ext:
            ext.build_optionset_cache()
            assert ext.lookup_optionset("teststatus", 999) is None

    def test_none_code_returns_none(self, source_db):
        with Extractor(source_db) as ext:
            ext.build_optionset_cache()
            assert ext.lookup_optionset("teststatus", None) is None

    def test_unknown_column_returns_none(self, source_db):
        with Extractor(source_db) as ext:
            ext.build_optionset_cache()
            assert ext.lookup_optionset("nonexistent", 1) is None


class TestExtractTable:
    def test_all_rows(self, source_db):
        with Extractor(source_db) as ext:
            rows = list(ext.extract_table("vin_candidates"))
            assert len(rows) == 3
            assert rows[0]["candidateid"] == "c1"

    def test_column_filter(self, source_db):
        with Extractor(source_db) as ext:
            rows = list(ext.extract_table("vin_candidates", ["candidateid"]))
            assert "candidateid" in rows[0]
            assert "new_currentrdstage" not in rows[0]


class TestExtractDistinct:
    def test_deduplicates(self, source_db):
        with Extractor(source_db) as ext:
            rows = list(ext.extract_distinct("vin_candidates", ["candidateid"]))
            ids = [r["candidateid"] for r in rows]
            assert sorted(ids) == ["c1", "c2"]


class TestGetTableColumns:
    def test_returns_columns(self, source_db):
        with Extractor(source_db) as ext:
            cols = ext.get_table_columns("vin_candidates")
            assert "candidateid" in cols
            assert "new_currentrdstage" in cols


class TestCountRows:
    def test_count(self, source_db):
        with Extractor(source_db) as ext:
            assert ext.count_rows("vin_candidates") == 3


class TestExecuteQuery:
    def test_arbitrary_select(self, source_db):
        with Extractor(source_db) as ext:
            rows = list(ext.execute_query("SELECT candidateid FROM vin_candidates WHERE candidateid = 'c1'"))
            assert len(rows) == 1
            assert rows[0]["candidateid"] == "c1"


class TestGetLastSyncDate:
    def test_returns_latest_with_changes(self, source_db):
        with Extractor(source_db) as ext:
            result = ext.get_last_sync_date()
            # Should return the one with rows_changed > 0
            assert result == "2025-03-01T10:00:00"

    def test_returns_none_when_no_matching_rows(self, tmp_path):
        """All sync entries have 0 changes — should return None."""
        db_path = tmp_path / "empty_sync.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE _sync_log (end_time TEXT, status TEXT, records_added INTEGER, records_updated INTEGER)")
        conn.execute("INSERT INTO _sync_log VALUES ('2025-01-01', 'completed', 0, 0)")
        conn.commit()
        conn.close()
        with Extractor(db_path) as ext:
            assert ext.get_last_sync_date() is None
