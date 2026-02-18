"""Tests for transformation logic."""

from unittest.mock import MagicMock

import pytest

from src.expressions import parse_case_when, parse_coalesce
from src.transformer import Transformer


class TestExpressionParsing:
    """Test expression parsing in Transformer."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor."""
        mock_extractor = MagicMock()
        mock_extractor.lookup_optionset = MagicMock(return_value="Approved")
        return Transformer(mock_extractor)

    def test_parse_coalesce_with_value(self):
        """COALESCE returns value when present."""
        row = {"col1": "actual_value"}
        result = parse_coalesce("COALESCE(col1, 'default')", row)
        assert result == "actual_value"

    def test_parse_coalesce_with_null(self):
        """COALESCE returns default when value is null."""
        row = {"col1": None}
        result = parse_coalesce("COALESCE(col1, 'default')", row)
        assert result == "default"

    def test_parse_coalesce_with_missing_column(self):
        """COALESCE returns default when column is missing."""
        row = {}
        result = parse_coalesce("COALESCE(col1, 'default')", row)
        assert result == "default"

    def test_parse_coalesce_null_literal(self):
        """COALESCE(NULL, 'value') returns value."""
        row = {}
        result = parse_coalesce("COALESCE(NULL, 'Unknown')", row)
        assert result == "Unknown"

    def test_parse_case_when_true(self):
        """CASE WHEN returns THEN value when condition is true."""
        row = {"statecode": 0}
        result = parse_case_when("CASE WHEN statecode = 0 THEN 1 ELSE 0 END", row)
        assert result == 1

    def test_parse_case_when_false(self):
        """CASE WHEN returns ELSE value when condition is false."""
        row = {"statecode": 1}
        result = parse_case_when("CASE WHEN statecode = 0 THEN 1 ELSE 0 END", row)
        assert result == 0

    def test_parse_case_when_null(self):
        """CASE WHEN returns ELSE value when column is null."""
        row = {"statecode": None}
        result = parse_case_when("CASE WHEN statecode = 0 THEN 1 ELSE 0 END", row)
        assert result == 0

    def test_parse_case_when_string_then(self):
        """CASE WHEN with string results returns THEN string when true."""
        row = {"vin_type": 909670000}
        result = parse_case_when("CASE WHEN vin_type = 909670000 THEN 'top-level' ELSE 'sub-product' END", row)
        assert result == "top-level"

    def test_parse_case_when_string_else(self):
        """CASE WHEN with string results returns ELSE string when false."""
        row = {"vin_type": 909670001}
        result = parse_case_when("CASE WHEN vin_type = 909670000 THEN 'top-level' ELSE 'sub-product' END", row)
        assert result == "sub-product"

    def test_parse_case_when_string_null_column(self):
        """CASE WHEN with string results returns ELSE when column is null."""
        row = {"vin_type": None}
        result = parse_case_when("CASE WHEN vin_type = 909670000 THEN 'top-level' ELSE 'sub-product' END", row)
        assert result == "sub-product"

    def test_parse_case_when_multi_branch_first_match(self):
        """Multi-branch CASE WHEN returns first matching THEN value."""
        expr = (
            "CASE WHEN captype_value = 'c1746ad3-93d1-f011-bbd3-00224892cefa' THEN 'Candidate' "
            "WHEN captype_value = '545d63d9-93d1-f011-bbd3-00224892cefa' THEN 'Product' "
            "ELSE 'Other' END"
        )
        row = {"captype_value": "c1746ad3-93d1-f011-bbd3-00224892cefa"}
        assert parse_case_when(expr, row) == "Candidate"

    def test_parse_case_when_multi_branch_second_match(self):
        """Multi-branch CASE WHEN returns second matching THEN value."""
        expr = (
            "CASE WHEN captype_value = 'c1746ad3-93d1-f011-bbd3-00224892cefa' THEN 'Candidate' "
            "WHEN captype_value = '545d63d9-93d1-f011-bbd3-00224892cefa' THEN 'Product' "
            "ELSE 'Other' END"
        )
        row = {"captype_value": "545d63d9-93d1-f011-bbd3-00224892cefa"}
        assert parse_case_when(expr, row) == "Product"

    def test_parse_case_when_multi_branch_else(self):
        """Multi-branch CASE WHEN returns ELSE when no branch matches."""
        expr = (
            "CASE WHEN captype_value = 'c1746ad3-93d1-f011-bbd3-00224892cefa' THEN 'Candidate' "
            "WHEN captype_value = '545d63d9-93d1-f011-bbd3-00224892cefa' THEN 'Product' "
            "ELSE 'Other' END"
        )
        row = {"captype_value": "some-other-guid"}
        assert parse_case_when(expr, row) == "Other"

    def test_parse_case_when_multi_branch_null(self):
        """Multi-branch CASE WHEN returns ELSE when column is null."""
        expr = (
            "CASE WHEN captype_value = 'c1746ad3-93d1-f011-bbd3-00224892cefa' THEN 'Candidate' "
            "WHEN captype_value = '545d63d9-93d1-f011-bbd3-00224892cefa' THEN 'Product' "
            "ELSE 'Other' END"
        )
        row = {"captype_value": None}
        assert parse_case_when(expr, row) == "Other"

    def test_parse_optionset(self, transformer):
        """OPTIONSET resolves code to label."""
        row = {"vin_approvalstatus": 909670000}
        result = transformer._parse_optionset("OPTIONSET:vin_approvalstatus", row)
        assert result == "Approved"
        transformer.extractor.lookup_optionset.assert_called_once_with("vin_approvalstatus", 909670000)

    def test_parse_optionset_null(self, transformer):
        """OPTIONSET returns None for null code."""
        row = {"vin_approvalstatus": None}
        result = transformer._parse_optionset("OPTIONSET:vin_approvalstatus", row)
        assert result is None

    def test_evaluate_simple_column(self, transformer):
        """Simple column reference returns value."""
        row = {"vin_name": "Test Candidate"}
        result = transformer._evaluate_expression("vin_name", row)
        assert result == "Test Candidate"

    def test_evaluate_missing_column(self, transformer):
        """Missing column returns None."""
        row = {}
        result = transformer._evaluate_expression("vin_name", row)
        assert result is None


class TestDimensionKeyCaching:
    """Test dimension key caching."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor."""
        mock_extractor = MagicMock()
        return Transformer(mock_extractor)

    def test_cache_dimension_keys(self, transformer):
        """Cache stores lookup values to keys."""
        data = [
            {"candidate_key": 1, "candidateid": "abc-123"},
            {"candidate_key": 2, "candidateid": "def-456"},
        ]
        transformer.cache_dimension_keys("dim_candidate_core", data, "candidate_key", "candidateid")

        assert transformer.lookup_dimension_key("dim_candidate_core", "abc-123") == 1
        assert transformer.lookup_dimension_key("dim_candidate_core", "def-456") == 2

    def test_lookup_missing_key(self, transformer):
        """Lookup returns None for unknown value."""
        transformer._dim_caches["dim_candidate_core"] = {}
        result = transformer.lookup_dimension_key("dim_candidate_core", "unknown")
        assert result is None

    def test_lookup_unknown_table(self, transformer):
        """Lookup returns None for unknown table."""
        result = transformer.lookup_dimension_key("unknown_table", "value")
        assert result is None

    def test_cache_composite_keys(self, transformer):
        """Composite key cache stores tuples."""
        data = [
            {
                "technology_key": 1,
                "platform": "mRNA",
                "technology_type": "Vaccine",
            },
            {
                "technology_key": 2,
                "platform": "Protein",
                "technology_type": "Drug",
            },
        ]
        transformer.cache_composite_keys("dim_candidate_tech", data, "technology_key", ["platform", "technology_type"])

        assert transformer.lookup_composite_key("dim_candidate_tech", ("mRNA", "Vaccine")) == 1
        assert transformer.lookup_composite_key("dim_candidate_tech", ("Protein", "Drug")) == 2


class TestDateDimensionGeneration:
    """Test date dimension generation."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor."""
        mock_extractor = MagicMock()
        return Transformer(mock_extractor)

    def test_generate_date_dimension_structure(self, transformer):
        """Date dimension has correct structure."""
        special = {"generate": True, "start_year": 2020, "end_year": 2020}
        result = transformer._generate_date_dimension(special)

        assert len(result) > 0
        first_row = result[0]
        assert "full_date" in first_row
        assert "year" in first_row
        assert "quarter" in first_row

    def test_generate_date_dimension_year_coverage(self, transformer):
        """Date dimension covers full year."""
        special = {"generate": True, "start_year": 2020, "end_year": 2020}
        result = transformer._generate_date_dimension(special)

        # 2020 is a leap year, so 366 days
        assert len(result) == 366

        # First date should be Jan 1
        assert result[0]["full_date"] == "2020-01-01"
        assert result[0]["year"] == 2020
        assert result[0]["quarter"] == 1

        # Last date should be Dec 31
        assert result[-1]["full_date"] == "2020-12-31"
        assert result[-1]["quarter"] == 4

    def test_generate_date_dimension_quarters(self, transformer):
        """Date dimension has correct quarters."""
        special = {"generate": True, "start_year": 2020, "end_year": 2020}
        result = transformer._generate_date_dimension(special)

        # Check Q1 (Jan-Mar)
        jan_dates = [r for r in result if r["full_date"].startswith("2020-01")]
        assert all(r["quarter"] == 1 for r in jan_dates)

        # Check Q2 (Apr-Jun)
        apr_dates = [r for r in result if r["full_date"].startswith("2020-04")]
        assert all(r["quarter"] == 2 for r in apr_dates)

        # Check Q3 (Jul-Sep)
        jul_dates = [r for r in result if r["full_date"].startswith("2020-07")]
        assert all(r["quarter"] == 3 for r in jul_dates)

        # Check Q4 (Oct-Dec)
        oct_dates = [r for r in result if r["full_date"].startswith("2020-10")]
        assert all(r["quarter"] == 4 for r in oct_dates)


class TestOptionsetDimension:
    """Test optionset-based dimension transformation."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor containing optionset data."""
        mock_extractor = MagicMock()
        mock_extractor._optionset_cache = {
            "agespecific": {
                909670000: "Neonates (<1 month)",
                909670001: "Infants (<1 year)",
                909670002: "Children (1-9 years)",
            }
        }
        return Transformer(mock_extractor)

    def test_transform_optionset_dimension(self, transformer):
        """Optionset dimension produces one row per option."""
        config = {
            "_source_table": None,
            "_pk": "age_group_key",
            "_special": {"from_optionset": True, "optionset_name": "agespecific"},
            "age_group_name": "OPTIONSET_LABEL",
            "option_code": "OPTIONSET_CODE",
        }
        special = config["_special"]
        result = transformer._transform_optionset_dimension("dim_age_group", config, special)

        assert len(result) == 3
        # Sorted by code, so first should be 909670000
        assert result[0]["age_group_name"] == "Neonates (<1 month)"
        assert result[0]["option_code"] == 909670000
        assert result[2]["age_group_name"] == "Children (1-9 years)"
        assert result[2]["option_code"] == 909670002

    def test_transform_optionset_dimension_empty(self, transformer):
        """Optionset dimension handles missing optionset gracefully."""
        config = {
            "_source_table": None,
            "_pk": "key",
            "_special": {"from_optionset": True, "optionset_name": "nonexistent"},
            "name": "OPTIONSET_LABEL",
            "option_code": "OPTIONSET_CODE",
        }
        special = config["_special"]
        result = transformer._transform_optionset_dimension("dim_test", config, special)
        assert result == []
