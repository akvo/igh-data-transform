"""Tests for silver_to_gold Loader."""

import sqlite3

import pytest

from igh_data_transform.transformations.silver_to_gold.core.loader import Loader


@pytest.fixture
def loader_path(tmp_path):
    return tmp_path / "gold.db"


class TestLoaderContextManager:
    def test_enter_exit(self, loader_path):
        with Loader(loader_path) as loader:
            assert loader is not None
        assert loader_path.exists()

    def test_removes_existing_db(self, loader_path):
        # Create a DB with some data
        loader_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(loader_path))
        conn.execute("CREATE TABLE old_table (id INTEGER)")
        conn.commit()
        conn.close()

        with Loader(loader_path) as loader:
            loader.create_schema()
        # old_table should not exist since loader recreates DB
        conn = sqlite3.connect(str(loader_path))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='old_table'"
        ).fetchall()}
        conn.close()
        assert "old_table" not in tables


class TestCreateSchema:
    def test_creates_tables(self, loader_path):
        with Loader(loader_path) as loader:
            loader.create_schema()

        conn = sqlite3.connect(str(loader_path))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()}
        conn.close()
        assert "dim_candidate_core" in tables
        assert "fact_pipeline_snapshot" in tables
        assert "bridge_candidate_developer" in tables


class TestLoadTable:
    def test_inserts_rows_with_autoincrement(self, loader_path):
        with Loader(loader_path) as loader:
            loader.create_schema()
            data = [
                {"candidateid": "c1", "candidate_name": "Drug A"},
                {"candidateid": "c2", "candidate_name": "Drug B"},
            ]
            result = loader.load_table("dim_candidate_core", data)
            assert len(result) == 2
            # Autoincrement keys should be assigned
            assert result[0]["candidate_key"] == 1
            assert result[1]["candidate_key"] == 2

    def test_empty_data(self, loader_path):
        with Loader(loader_path) as loader:
            loader.create_schema()
            result = loader.load_table("dim_candidate_core", [])
            assert result == []


class TestGetRowCount:
    def test_count_after_load(self, loader_path):
        with Loader(loader_path) as loader:
            loader.create_schema()
            loader.load_table("dim_disease", [
                {"diseaseid": "d1", "disease_name": "Malaria"},
                {"diseaseid": "d2", "disease_name": "TB"},
            ])
            assert loader.get_row_count("dim_disease") == 2


class TestWriteMetadata:
    def test_writes_key_value_pairs(self, loader_path):
        with Loader(loader_path) as loader:
            loader.create_schema()
            loader.write_metadata({"last_sync_date": "2025-03-01", "version": "1.0"})

        conn = sqlite3.connect(str(loader_path))
        rows = conn.execute("SELECT key, value FROM _etl_metadata ORDER BY key").fetchall()
        conn.close()
        metadata = dict(rows)
        assert metadata["last_sync_date"] == "2025-03-01"
        assert metadata["version"] == "1.0"


class TestCreateIndexes:
    def test_creates_indexes_without_error(self, loader_path):
        with Loader(loader_path) as loader:
            loader.create_schema()
            # Load some minimal data so tables exist
            loader.create_indexes()

        conn = sqlite3.connect(str(loader_path))
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        conn.close()
        assert len(indexes) > 0
