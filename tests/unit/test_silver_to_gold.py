"""Tests for silver_to_gold transformation."""

import inspect
import sys
from pathlib import Path
from unittest.mock import MagicMock

from igh_data_transform.transformations.silver_to_gold import silver_to_gold


class TestSilverToGold:
    """Tests for silver_to_gold function."""

    def test_function_signature(self) -> None:
        """Test that silver_to_gold has the correct signature."""
        sig = inspect.signature(silver_to_gold)
        params = list(sig.parameters.keys())
        assert params == ["silver_db_path", "gold_db_path"]
        assert sig.return_annotation is bool

    def test_gold_db_path_defaults_to_none(self) -> None:
        """Test that gold_db_path has a default of None."""
        sig = inspect.signature(silver_to_gold)
        assert sig.parameters["gold_db_path"].default is None

    def test_delegates_to_run_etl(self, tmp_path: Path) -> None:
        """Test that silver_to_gold delegates to run_etl with correct paths."""
        # Get the actual package module (not the shadowed function)
        stg = sys.modules["igh_data_transform.transformations.silver_to_gold"]
        original_run_etl = stg.run_etl
        mock_run_etl = MagicMock(return_value=True)
        try:
            stg.run_etl = mock_run_etl

            silver_db = tmp_path / "silver.db"
            gold_db = tmp_path / "gold.db"
            result = stg.silver_to_gold(str(silver_db), str(gold_db))

            assert result is True
            mock_run_etl.assert_called_once_with(silver_db, gold_db)
        finally:
            stg.run_etl = original_run_etl

    def test_default_gold_db_path(self, tmp_path: Path) -> None:
        """Test that gold_db_path defaults to star_schema.db next to silver DB."""
        stg = sys.modules["igh_data_transform.transformations.silver_to_gold"]
        original_run_etl = stg.run_etl
        mock_run_etl = MagicMock(return_value=True)
        try:
            stg.run_etl = mock_run_etl

            silver_db = tmp_path / "silver.db"
            result = stg.silver_to_gold(str(silver_db))

            assert result is True
            expected_gold = silver_db.parent / "star_schema.db"
            mock_run_etl.assert_called_once_with(silver_db, expected_gold)
        finally:
            stg.run_etl = original_run_etl
