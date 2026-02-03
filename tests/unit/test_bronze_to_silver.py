"""Tests for bronze_to_silver transformation orchestration."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from igh_data_transform.transformations.bronze_to_silver import (
    bronze_to_silver,
    transform_table,
)


class TestTransformTable:
    """Tests for transform_table function."""

    def test_drops_empty_columns(self):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [None, None, None],
                "c": ["x", "y", "z"],
            }
        )
        result = transform_table(df)
        assert "a" in result.columns
        assert "b" not in result.columns
        assert "c" in result.columns

    def test_preserves_valid_to_column(self):
        df = pd.DataFrame(
            {
                "a": [1, 2],
                "valid_to": [None, None],
            }
        )
        result = transform_table(df)
        assert "valid_to" in result.columns

    def test_preserves_custom_columns(self):
        df = pd.DataFrame(
            {
                "a": [1, 2],
                "keep_me": [None, None],
            }
        )
        result = transform_table(df, preserve_columns=["keep_me"])
        assert "keep_me" in result.columns

    def test_renames_columns(self):
        df = pd.DataFrame({"old_name": [1, 2]})
        result = transform_table(df, column_renames={"old_name": "new_name"})
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_normalizes_text_columns(self):
        df = pd.DataFrame({"text": ["  hello  ", "world<br>test"]})
        result = transform_table(df, text_columns=["text"])
        assert list(result["text"]) == ["hello", "world test"]

    def test_replaces_values(self):
        df = pd.DataFrame({"status": ["Active", "Inactive"]})
        result = transform_table(
            df,
            value_mappings={"status": {"Active": "A", "Inactive": "I"}},
        )
        assert list(result["status"]) == ["A", "I"]

    def test_combined_transformations(self):
        df = pd.DataFrame(
            {
                "vin_name": ["  Test Product  ", "Another<br>Product"],
                "empty_col": [None, None],
                "status": ["Active", "Inactive"],
                "valid_to": [None, None],
            }
        )
        result = transform_table(
            df,
            column_renames={"vin_name": "name"},
            text_columns=["name"],
            value_mappings={"status": {"Active": "A", "Inactive": "I"}},
        )
        assert "name" in result.columns
        assert "vin_name" not in result.columns
        assert "empty_col" not in result.columns
        assert "valid_to" in result.columns
        assert list(result["name"]) == ["Test Product", "Another Product"]
        assert list(result["status"]) == ["A", "I"]

    def test_skips_missing_text_columns(self):
        df = pd.DataFrame({"a": [1, 2]})
        # Should not raise error for non-existent column
        result = transform_table(df, text_columns=["nonexistent"])
        assert list(result.columns) == ["a"]

    def test_skips_missing_value_mapping_columns(self):
        df = pd.DataFrame({"a": [1, 2]})
        # Should not raise error for non-existent column
        result = transform_table(df, value_mappings={"nonexistent": {1: 2}})
        assert list(result.columns) == ["a"]


class TestBronzeToSilver:
    """Tests for bronze_to_silver orchestration function."""

    @pytest.fixture
    def bronze_db(self, tmp_path: Path) -> Path:
        """Create a Bronze database with test data."""
        db_path = tmp_path / "bronze.db"
        conn = sqlite3.connect(str(db_path))

        # Create test table with typical Bronze layer structure
        conn.execute("""
            CREATE TABLE vin_candidates (
                row_id INTEGER PRIMARY KEY,
                vin_name TEXT,
                status TEXT,
                empty_col TEXT,
                valid_from TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO vin_candidates (row_id, vin_name, status, empty_col, valid_from, valid_to)
            VALUES
                (1, '  Product A  ', 'Active', NULL, '2024-01-01', NULL),
                (2, 'Product<br>B', 'Inactive', NULL, '2024-01-02', NULL)
        """)

        # Create a second table
        conn.execute("""
            CREATE TABLE vin_diseases (
                row_id INTEGER PRIMARY KEY,
                disease_name TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO vin_diseases (row_id, disease_name, valid_to)
            VALUES (1, 'Disease A', NULL)
        """)

        conn.commit()
        conn.close()
        return db_path

    @pytest.fixture
    def silver_db_path(self, tmp_path: Path) -> Path:
        """Return path for Silver database."""
        return tmp_path / "silver.db"

    def test_transforms_bronze_to_silver(self, bronze_db: Path, silver_db_path: Path):
        """Test basic transformation from Bronze to Silver."""
        result = bronze_to_silver(str(bronze_db), str(silver_db_path))

        assert result is True
        assert silver_db_path.exists()

        # Verify Silver database contents
        conn = sqlite3.connect(str(silver_db_path))
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = [t[0] for t in tables]

        assert "vin_candidates" in table_names
        assert "vin_diseases" in table_names
        conn.close()

    def test_drops_empty_columns_in_silver(self, bronze_db: Path, silver_db_path: Path):
        """Test that empty columns are dropped in Silver."""
        bronze_to_silver(str(bronze_db), str(silver_db_path))

        conn = sqlite3.connect(str(silver_db_path))
        cursor = conn.execute("PRAGMA table_info(vin_candidates)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()

        assert "empty_col" not in columns
        assert "valid_to" in columns  # Preserved even though empty

    def test_preserves_row_count(self, bronze_db: Path, silver_db_path: Path):
        """Test that row count is preserved."""
        bronze_to_silver(str(bronze_db), str(silver_db_path))

        conn = sqlite3.connect(str(silver_db_path))
        count = conn.execute("SELECT COUNT(*) FROM vin_candidates").fetchone()[0]
        conn.close()

        assert count == 2

    def test_returns_true_for_empty_database(self, tmp_path: Path):
        """Test handling of empty Bronze database."""
        empty_db = tmp_path / "empty.db"
        sqlite3.connect(str(empty_db)).close()  # Create empty db

        silver_db = tmp_path / "silver.db"
        result = bronze_to_silver(str(empty_db), str(silver_db))

        assert result is True

    def test_returns_false_on_error(self, tmp_path: Path):
        """Test that errors return False."""
        # Use a path inside a nonexistent directory to trigger an error
        nonexistent = tmp_path / "nonexistent_dir" / "bronze.db"
        silver_db = tmp_path / "silver.db"

        result = bronze_to_silver(str(nonexistent), str(silver_db))

        assert result is False

    def test_creates_silver_database(self, bronze_db: Path, tmp_path: Path):
        """Test that Silver database is created if it doesn't exist."""
        silver_db = tmp_path / "new_silver.db"
        assert not silver_db.exists()

        bronze_to_silver(str(bronze_db), str(silver_db))

        assert silver_db.exists()

    def test_overwrites_existing_silver_tables(self, bronze_db: Path, silver_db_path: Path):
        """Test that existing Silver tables are replaced."""
        # Create Silver with different data
        conn = sqlite3.connect(str(silver_db_path))
        conn.execute("CREATE TABLE vin_candidates (old_col TEXT)")
        conn.execute("INSERT INTO vin_candidates VALUES ('old_data')")
        conn.commit()
        conn.close()

        # Run transformation
        bronze_to_silver(str(bronze_db), str(silver_db_path))

        # Verify old data is replaced
        conn = sqlite3.connect(str(silver_db_path))
        cursor = conn.execute("PRAGMA table_info(vin_candidates)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()

        assert "old_col" not in columns
        assert "vin_name" in columns
