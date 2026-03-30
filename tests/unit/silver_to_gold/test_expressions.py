"""Tests for silver_to_gold expression parsing."""

from igh_data_transform.transformations.silver_to_gold.core.expressions import (
    _eval_single_branch_case_when,
    evaluate_lookup,
    parse_case_when,
    parse_coalesce,
)
from igh_data_transform.transformations.silver_to_gold.config.phase_sort_order import (
    DEFAULT_SORT_ORDER,
)


# =========================================================================
# parse_coalesce
# =========================================================================


class TestParseCoalesce:
    def test_string_default_column_present(self):
        assert parse_coalesce("COALESCE(name, 'Unknown')", {"name": "Alice"}) == "Alice"

    def test_string_default_column_none(self):
        assert parse_coalesce("COALESCE(name, 'Unknown')", {"name": None}) == "Unknown"

    def test_string_default_column_missing(self):
        assert parse_coalesce("COALESCE(name, 'Unknown')", {}) == "Unknown"

    def test_integer_default_column_present(self):
        assert parse_coalesce("COALESCE(count, 0)", {"count": 42}) == 42

    def test_integer_default_column_none(self):
        assert parse_coalesce("COALESCE(count, 0)", {"count": None}) == 0

    def test_null_with_default(self):
        assert parse_coalesce("COALESCE(NULL, 'Unknown')", {}) == "Unknown"

    def test_two_column_refs_first_present(self):
        assert parse_coalesce("COALESCE(a, b)", {"a": "X", "b": "Y"}) == "X"

    def test_two_column_refs_first_none(self):
        assert parse_coalesce("COALESCE(a, b)", {"a": None, "b": "Y"}) == "Y"

    def test_two_column_refs_both_none(self):
        assert parse_coalesce("COALESCE(a, b)", {"a": None, "b": None}) is None

    def test_unrecognized_returns_none(self):
        assert parse_coalesce("NOT_A_COALESCE", {}) is None


# =========================================================================
# _eval_single_branch_case_when
# =========================================================================


class TestEvalSingleBranchCaseWhen:
    # Integer equality
    def test_int_equality_match(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN statecode = 0 THEN 1 ELSE 0 END", {"statecode": 0}
        )
        assert matched is True
        assert val == 1

    def test_int_equality_no_match(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN statecode = 0 THEN 1 ELSE 0 END", {"statecode": 5}
        )
        assert matched is True
        assert val == 0

    def test_int_equality_none(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN statecode = 0 THEN 1 ELSE 0 END", {"statecode": None}
        )
        assert matched is True
        assert val == 0

    # IS NULL
    def test_is_null_when_null(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN col IS NULL THEN 1 ELSE 0 END", {"col": None}
        )
        assert matched is True
        assert val == 1

    def test_is_null_when_not_null(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN col IS NULL THEN 1 ELSE 0 END", {"col": "present"}
        )
        assert matched is True
        assert val == 0

    # String results
    def test_string_result_match(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN vin_type = 909670000 THEN 'top-level' ELSE 'sub-product' END",
            {"vin_type": 909670000},
        )
        assert matched is True
        assert val == "top-level"

    def test_string_result_no_match(self):
        matched, val = _eval_single_branch_case_when(
            "CASE WHEN vin_type = 909670000 THEN 'top-level' ELSE 'sub-product' END",
            {"vin_type": 1},
        )
        assert matched is True
        assert val == "sub-product"

    # Unrecognized
    def test_unrecognized(self):
        matched, val = _eval_single_branch_case_when("NOT A CASE WHEN", {})
        assert matched is False
        assert val is None


# =========================================================================
# parse_case_when
# =========================================================================


class TestParseCaseWhen:
    def test_single_branch_routed(self):
        result = parse_case_when(
            "CASE WHEN statecode = 0 THEN 1 ELSE 0 END", {"statecode": 0}
        )
        assert result == 1

    def test_multi_branch_first_match(self):
        expr = (
            "CASE WHEN captype_value = 'abc' THEN 'Candidate' "
            "WHEN captype_value = 'xyz' THEN 'Product' "
            "ELSE 'Other' END"
        )
        assert parse_case_when(expr, {"captype_value": "abc"}) == "Candidate"

    def test_multi_branch_second_match(self):
        expr = (
            "CASE WHEN captype_value = 'abc' THEN 'Candidate' "
            "WHEN captype_value = 'xyz' THEN 'Product' "
            "ELSE 'Other' END"
        )
        assert parse_case_when(expr, {"captype_value": "xyz"}) == "Product"

    def test_multi_branch_else(self):
        expr = (
            "CASE WHEN captype_value = 'abc' THEN 'Candidate' "
            "WHEN captype_value = 'xyz' THEN 'Product' "
            "ELSE 'Other' END"
        )
        assert parse_case_when(expr, {"captype_value": "zzz"}) == "Other"

    def test_unrecognized_returns_none(self):
        assert parse_case_when("GARBAGE", {}) is None


# =========================================================================
# evaluate_lookup
# =========================================================================


class TestEvaluateLookup:
    def test_phase_sort_order_known(self):
        result = evaluate_lookup(
            "LOOKUP:PHASE_SORT_ORDER", {"vin_name": "Phase I"}
        )
        assert result == 30

    def test_phase_sort_order_unknown(self):
        result = evaluate_lookup(
            "LOOKUP:PHASE_SORT_ORDER", {"vin_name": "Unknown Phase"}
        )
        assert result == DEFAULT_SORT_ORDER

    def test_country_iso_code(self):
        result = evaluate_lookup(
            "LOOKUP:COUNTRY_ISO_CODE", {"vin_name": "France"}
        )
        assert result == "FRA"

    def test_country_iso_code_empty(self):
        result = evaluate_lookup("LOOKUP:COUNTRY_ISO_CODE", {"vin_name": ""})
        assert result is None

    def test_unknown_lookup_type(self):
        result = evaluate_lookup("LOOKUP:UNKNOWN", {"vin_name": "test"})
        assert result is None
