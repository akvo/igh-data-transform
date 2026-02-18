"""Tests for bronze_to_silver transformation orchestration."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from igh_data_transform.transformations.bronze_to_silver import (
    OPTIONSET_RENAMES,
    TABLE_REGISTRY,
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
        """Create a Bronze database with test data (generic tables)."""
        db_path = tmp_path / "bronze.db"
        conn = sqlite3.connect(str(db_path))

        # Use non-registered table names for generic fallthrough testing
        conn.execute("""
            CREATE TABLE some_table (
                row_id INTEGER PRIMARY KEY,
                vin_name TEXT,
                status TEXT,
                empty_col TEXT,
                valid_from TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO some_table (row_id, vin_name, status, empty_col, valid_from, valid_to)
            VALUES
                (1, '  Product A  ', 'Active', NULL, '2024-01-01', NULL),
                (2, 'Product B', 'Inactive', NULL, '2024-01-02', NULL)
        """)

        # Create a second generic table
        conn.execute("""
            CREATE TABLE another_table (
                row_id INTEGER PRIMARY KEY,
                disease_name TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO another_table (row_id, disease_name, valid_to)
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
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        assert "some_table" in table_names
        assert "another_table" in table_names
        conn.close()

    def test_drops_empty_columns_in_silver(self, bronze_db: Path, silver_db_path: Path):
        """Test that empty columns are dropped in Silver."""
        bronze_to_silver(str(bronze_db), str(silver_db_path))

        conn = sqlite3.connect(str(silver_db_path))
        cursor = conn.execute("PRAGMA table_info(some_table)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()

        assert "empty_col" not in columns
        assert "valid_to" in columns  # Preserved even though empty

    def test_preserves_row_count(self, bronze_db: Path, silver_db_path: Path):
        """Test that row count is preserved."""
        bronze_to_silver(str(bronze_db), str(silver_db_path))

        conn = sqlite3.connect(str(silver_db_path))
        count = conn.execute("SELECT COUNT(*) FROM some_table").fetchone()[0]
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

    def test_overwrites_existing_silver_tables(
        self, bronze_db: Path, silver_db_path: Path
    ):
        """Test that existing Silver tables are replaced."""
        # Create Silver with different data
        conn = sqlite3.connect(str(silver_db_path))
        conn.execute("CREATE TABLE some_table (old_col TEXT)")
        conn.execute("INSERT INTO some_table VALUES ('old_data')")
        conn.commit()
        conn.close()

        # Run transformation
        bronze_to_silver(str(bronze_db), str(silver_db_path))

        # Verify old data is replaced
        conn = sqlite3.connect(str(silver_db_path))
        cursor = conn.execute("PRAGMA table_info(some_table)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()

        assert "old_col" not in columns
        assert "vin_name" in columns


class TestRegistryDispatch:
    """Tests for table-specific transformer dispatch."""

    def _create_priorities_db(self, db_path: Path) -> None:
        """Create a Bronze DB with vin_rdpriorities and a generic table."""
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE vin_rdpriorities (
                row_id INTEGER,
                vin_name TEXT,
                new_author TEXT,
                new_safety TEXT,
                json_response TEXT,
                sync_time TEXT,
                valid_from TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO vin_rdpriorities VALUES
                (1, 'Priority A', 'World Health Organization', 'Safe',
                 '{"k":"v"}', '2026-01-09', '2025-01-01', NULL)
        """)
        # Generic table that should fall through
        conn.execute("""
            CREATE TABLE generic_table (
                col_a TEXT,
                col_b TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("INSERT INTO generic_table VALUES ('x', NULL, NULL)")
        conn.commit()
        conn.close()

    def test_registry_contains_expected_tables(self):
        assert "vin_candidates" in TABLE_REGISTRY
        assert "vin_clinicaltrials" in TABLE_REGISTRY
        assert "vin_diseases" in TABLE_REGISTRY
        assert "vin_rdpriorities" in TABLE_REGISTRY

    def test_registered_table_dispatches_to_transformer(self, tmp_path: Path):
        """Registered table uses its specific transformer."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        self._create_priorities_db(bronze_db)

        result = bronze_to_silver(str(bronze_db), str(silver_db))
        assert result is True

        conn = sqlite3.connect(str(silver_db))
        df = pd.read_sql_query("SELECT * FROM vin_rdpriorities", conn)
        conn.close()

        # Priorities transformer renames vin_name -> name
        assert "name" in df.columns
        assert "vin_name" not in df.columns
        # Priorities transformer maps "World Health Organization" -> "WHO"
        assert df["author"].iloc[0] == "WHO"
        # Metadata columns dropped
        assert "row_id" not in df.columns
        assert "json_response" not in df.columns

    def test_unregistered_table_uses_generic_transform(self, tmp_path: Path):
        """Unregistered tables fall through to generic transform_table."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        self._create_priorities_db(bronze_db)

        bronze_to_silver(str(bronze_db), str(silver_db))

        conn = sqlite3.connect(str(silver_db))
        df = pd.read_sql_query("SELECT * FROM generic_table", conn)
        conn.close()

        # Generic transform drops all-null columns
        assert "col_b" not in df.columns
        # Preserves valid_to
        assert "valid_to" in df.columns
        assert "col_a" in df.columns

    def test_option_sets_loaded_and_passed_to_transformer(self, tmp_path: Path):
        """Option sets from Bronze are loaded and passed to transformers."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        conn = sqlite3.connect(str(bronze_db))

        # Create diseases table
        conn.execute("""
            CREATE TABLE vin_diseases (
                vin_disease TEXT,
                vin_name TEXT,
                new_globalhealtharea INTEGER,
                vin_diseaseid TEXT,
                valid_from TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO vin_diseases VALUES
                ('Malaria', 'Disease A', 100000002, 'did-1', '2025-01-01', NULL)
        """)

        # Create the option set table
        conn.execute("""
            CREATE TABLE _optionset_new_globalhealtharea (
                code INTEGER,
                label TEXT,
                first_seen TEXT
            )
        """)
        conn.execute("""
            INSERT INTO _optionset_new_globalhealtharea VALUES
                (100000000, 'Neglected disease', '2026-01-09'),
                (100000002, 'Sexual & reproductive health', '2026-01-09')
        """)

        conn.commit()
        conn.close()

        bronze_to_silver(str(bronze_db), str(silver_db))

        # Verify the cleaned option set was written to Silver with renamed table
        conn = sqlite3.connect(str(silver_db))
        os_df = pd.read_sql_query("SELECT * FROM _optionset_globalhealtharea", conn)
        conn.close()

        # Diseases transformer renames "Sexual & reproductive health" -> "Womens Health"
        labels = list(os_df["label"])
        assert "Womens Health" in labels
        assert "Sexual & reproductive health" not in labels

    def test_unmodified_option_set_copied_as_is(self, tmp_path: Path):
        """Option set tables not touched by a transformer are copied as-is."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        conn = sqlite3.connect(str(bronze_db))

        # Only generic tables (no registered tables that use this option set)
        conn.execute("""
            CREATE TABLE _optionset_some_field (
                code INTEGER,
                label TEXT,
                first_seen TEXT
            )
        """)
        conn.execute("""
            INSERT INTO _optionset_some_field VALUES
                (1, 'Label A', '2026-01-09'),
                (2, 'Label B', '2026-01-09')
        """)
        conn.commit()
        conn.close()

        bronze_to_silver(str(bronze_db), str(silver_db))

        conn = sqlite3.connect(str(silver_db))
        os_df = pd.read_sql_query("SELECT * FROM _optionset_some_field", conn)
        conn.close()

        assert len(os_df) == 2
        assert list(os_df["label"]) == ["Label A", "Label B"]

    def test_cleaned_option_set_overrides_raw_copy(self, tmp_path: Path):
        """When a transformer cleans an option set, the cleaned version is written."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        conn = sqlite3.connect(str(bronze_db))

        # Create diseases table
        conn.execute("""
            CREATE TABLE vin_diseases (
                vin_disease TEXT,
                vin_name TEXT,
                new_globalhealtharea INTEGER,
                vin_diseaseid TEXT,
                valid_from TEXT,
                valid_to TEXT
            )
        """)
        conn.execute("""
            INSERT INTO vin_diseases VALUES
                ('Malaria', 'Disease A', 100000000, 'did-1', '2025-01-01', NULL)
        """)

        # Option set also exists as a separate table in Bronze
        conn.execute("""
            CREATE TABLE _optionset_new_globalhealtharea (
                code INTEGER,
                label TEXT,
                first_seen TEXT
            )
        """)
        conn.execute("""
            INSERT INTO _optionset_new_globalhealtharea VALUES
                (100000000, 'Neglected disease', '2026-01-09'),
                (100000002, 'Sexual & reproductive health', '2026-01-09')
        """)

        conn.commit()
        conn.close()

        bronze_to_silver(str(bronze_db), str(silver_db))

        # The cleaned version should be in Silver with renamed table
        conn = sqlite3.connect(str(silver_db))
        os_df = pd.read_sql_query("SELECT * FROM _optionset_globalhealtharea", conn)
        conn.close()

        labels = list(os_df["label"])
        assert "Womens Health" in labels
        assert "Sexual & reproductive health" not in labels


class TestOptionsetRenaming:
    """Tests for optionset table renaming in Bronze to Silver."""

    def test_optionset_renames_mapping_exists(self):
        """OPTIONSET_RENAMES contains expected entries."""
        assert "_optionset_vin_approvalstatus" in OPTIONSET_RENAMES
        assert (
            OPTIONSET_RENAMES["_optionset_vin_approvalstatus"]
            == "_optionset_approvalstatus"
        )
        assert "_optionset_new_globalhealtharea" in OPTIONSET_RENAMES
        assert (
            OPTIONSET_RENAMES["_optionset_new_globalhealtharea"]
            == "_optionset_globalhealtharea"
        )

    def test_renamed_optionset_tables_appear_in_silver(self, tmp_path: Path):
        """Optionset tables are renamed when written to Silver."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        conn = sqlite3.connect(str(bronze_db))

        # Create optionset table with bronze name
        conn.execute("""
            CREATE TABLE _optionset_vin_developmentstatus (
                code INTEGER,
                label TEXT,
                first_seen TEXT
            )
        """)
        conn.execute("""
            INSERT INTO _optionset_vin_developmentstatus VALUES
                (909670000, 'Active', '2026-01-09')
        """)
        conn.commit()
        conn.close()

        bronze_to_silver(str(bronze_db), str(silver_db))

        conn = sqlite3.connect(str(silver_db))
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        conn.close()

        # Should have renamed table, not original
        assert "_optionset_developmentstatus" in tables
        assert "_optionset_vin_developmentstatus" not in tables

    def test_unrenamed_optionset_tables_kept_as_is(self, tmp_path: Path):
        """Optionset tables not in OPTIONSET_RENAMES are copied with original name."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        conn = sqlite3.connect(str(bronze_db))

        conn.execute("""
            CREATE TABLE _optionset_unknown_field (
                code INTEGER,
                label TEXT,
                first_seen TEXT
            )
        """)
        conn.execute("""
            INSERT INTO _optionset_unknown_field VALUES (1, 'Label', '2026-01-09')
        """)
        conn.commit()
        conn.close()

        bronze_to_silver(str(bronze_db), str(silver_db))

        conn = sqlite3.connect(str(silver_db))
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        conn.close()

        assert "_optionset_unknown_field" in tables
