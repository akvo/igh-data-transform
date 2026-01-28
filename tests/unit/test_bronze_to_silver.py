"""Tests for bronze_to_silver transformation."""

from pathlib import Path

from igh_data_transform.transformations.bronze_to_silver import bronze_to_silver


class TestBronzeToSilver:
    """Tests for bronze_to_silver function."""

    def test_returns_true(self, tmp_path: Path) -> None:
        """Test that bronze_to_silver returns True (placeholder behavior)."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        result = bronze_to_silver(str(bronze_db), str(silver_db))
        assert result is True

    def test_accepts_string_paths(self) -> None:
        """Test that bronze_to_silver accepts string paths."""
        result = bronze_to_silver("/path/to/bronze.db", "/path/to/silver.db")
        assert result is True

    def test_function_signature(self) -> None:
        """Test that bronze_to_silver has the correct signature."""
        import inspect

        sig = inspect.signature(bronze_to_silver)
        params = list(sig.parameters.keys())
        assert params == ["bronze_db_path", "silver_db_path"]
        assert sig.return_annotation is bool
