"""Tests for data quality validators."""

from pathlib import Path

from igh_data_transform.utils.validators import validate_data_quality


class TestValidateDataQuality:
    """Tests for validate_data_quality function."""

    def test_returns_empty_list(self, tmp_path: Path) -> None:
        """Test that validate_data_quality returns empty list (placeholder behavior)."""
        db_path = tmp_path / "test.db"
        result = validate_data_quality(str(db_path))
        assert result == []
        assert isinstance(result, list)

    def test_accepts_validation_rules(self, tmp_path: Path) -> None:
        """Test that validate_data_quality accepts validation_rules argument."""
        db_path = tmp_path / "test.db"
        result = validate_data_quality(str(db_path), validation_rules=["rule1", "rule2"])
        assert result == []

    def test_validation_rules_default_none(self, tmp_path: Path) -> None:
        """Test that validation_rules defaults to None."""
        db_path = tmp_path / "test.db"
        # Should work without validation_rules argument
        result = validate_data_quality(str(db_path))
        assert result == []

    def test_function_signature(self) -> None:
        """Test that validate_data_quality has the correct signature."""
        import inspect

        sig = inspect.signature(validate_data_quality)
        params = list(sig.parameters.keys())
        assert params == ["db_path", "validation_rules"]
        assert sig.return_annotation == list[dict]
