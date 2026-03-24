"""Tests for silver_to_gold facade."""

import inspect
import sys
from pathlib import Path
from unittest.mock import patch

from igh_data_transform.transformations.silver_to_gold import silver_to_gold

# The transformations __init__.py shadows the silver_to_gold package with
# the function of the same name.  Access the actual module via sys.modules.
_stg_pkg = sys.modules["igh_data_transform.transformations.silver_to_gold"]


class TestSilverToGold:
    """Tests for silver_to_gold function."""

    def test_function_signature(self) -> None:
        """Test that silver_to_gold has the correct signature."""
        sig = inspect.signature(silver_to_gold)
        params = list(sig.parameters.keys())
        assert params == ["silver_db_path", "gold_db_path"]
        assert sig.return_annotation is bool

    @patch.object(_stg_pkg, "run_etl")
    def test_delegates_to_run_etl(self, mock_run_etl, tmp_path: Path) -> None:
        """Test that silver_to_gold delegates to run_etl with correct paths."""
        mock_run_etl.return_value = True
        silver_db = tmp_path / "silver.db"
        gold_db = tmp_path / "gold.db"

        result = silver_to_gold(str(silver_db), str(gold_db))

        assert result is True
        mock_run_etl.assert_called_once_with(silver_db, gold_db)

    @patch.object(_stg_pkg, "run_etl")
    def test_returns_false_on_failure(self, mock_run_etl, tmp_path: Path) -> None:
        """Test that silver_to_gold returns False when run_etl fails."""
        mock_run_etl.return_value = False
        silver_db = tmp_path / "silver.db"
        gold_db = tmp_path / "gold.db"

        result = silver_to_gold(str(silver_db), str(gold_db))

        assert result is False
