"""Tests for silver_to_gold year expansion logic."""

from igh_data_transform.transformations.silver_to_gold.core.year_expansion import (
    _collect_reporting_years,
    _infill_for_row,
    _parse_year,
    expand_pipeline_years,
)


def _key_fn(date_str):
    """Simple lookup_date_key mock: returns year as int."""
    return int(date_str[:4])


# =========================================================================
# _parse_year
# =========================================================================


class TestParseYear:
    def test_iso_date(self):
        assert _parse_year("2024-01-15") == 2024

    def test_iso_timestamp(self):
        assert _parse_year("2023-06-01T12:00:00") == 2023

    def test_none(self):
        assert _parse_year(None) is None

    def test_empty_string(self):
        assert _parse_year("") is None

    def test_short_string(self):
        assert _parse_year("20") is None

    def test_non_numeric(self):
        assert _parse_year("abcd-01-01") is None


# =========================================================================
# _collect_reporting_years
# =========================================================================


class TestCollectReportingYears:
    def test_basic(self):
        ranges = [("2023-01-01", "2025-12-31")]
        assert _collect_reporting_years(ranges) == {2023, 2025}

    def test_multiple_ranges(self):
        ranges = [
            ("2021-01-01", "2023-12-31"),
            ("2024-01-01", None),
        ]
        assert _collect_reporting_years(ranges) == {2021, 2023, 2024}

    def test_none_values(self):
        ranges = [(None, None)]
        assert _collect_reporting_years(ranges) == set()

    def test_empty(self):
        assert _collect_reporting_years([]) == set()


# =========================================================================
# _infill_for_row
# =========================================================================


class TestInfillForRow:
    def test_no_infill_needed(self):
        row = {"candidate_key": 1, "date_key": 2023, "is_active_flag": 0}
        result = _infill_for_row(row, "2023-01-01", "2024-01-01", [2023, 2024, 2025], 2025, _key_fn)
        assert len(result) == 1
        assert result[0] is row

    def test_infill_closed_record(self):
        row = {"candidate_key": 1, "date_key": 2021, "is_active_flag": 0}
        result = _infill_for_row(
            row, "2021-01-01", "2024-01-01", [2021, 2022, 2023, 2024], 2024, _key_fn
        )
        # Expect original + infills for 2022, 2023
        assert len(result) == 3
        assert result[0] is row
        assert result[1]["date_key"] == 2022
        assert result[2]["date_key"] == 2023
        # Closed record: all infills have is_active_flag=0
        assert all(r["is_active_flag"] == 0 for r in result)

    def test_infill_active_record(self):
        row = {"candidate_key": 1, "date_key": 2022, "is_active_flag": 1}
        result = _infill_for_row(
            row, "2022-01-01", None, [2022, 2023, 2024, 2025], 2025, _key_fn
        )
        # active (vt=None): infills for 2023, 2024, 2025
        assert len(result) == 4
        # Original flipped to inactive
        assert result[0]["is_active_flag"] == 0
        # Last infill gets active flag
        assert result[-1]["is_active_flag"] == 1
        assert result[-1]["date_key"] == 2025

    def test_no_from_year(self):
        row = {"candidate_key": 1, "date_key": None, "is_active_flag": 0}
        result = _infill_for_row(row, None, None, [2023, 2024], 2024, _key_fn)
        assert len(result) == 1
        assert result[0] is row


# =========================================================================
# expand_pipeline_years
# =========================================================================


class TestExpandPipelineYears:
    def test_basic_expansion(self):
        """Expansion only creates infill for reporting years between from and to."""
        transformed = [
            {"candidate_key": 1, "date_key": 2021, "is_active_flag": 0},
            {"candidate_key": 2, "date_key": 2024, "is_active_flag": 0},
        ]
        # Reporting years derived: {2021, 2024, 2025}
        ranges = [("2021-01-01", "2025-01-01"), ("2024-01-01", "2025-12-31")]
        result = expand_pipeline_years(transformed, ranges, _key_fn)
        # Row 1: from=2021, to=2025; infill where 2021 < y < 2025 → {2024}
        # Row 2: from=2024, to=2025; infill where 2024 < y < 2025 → none
        assert len(result) == 3

    def test_no_expansion_single_year(self):
        transformed = [
            {"candidate_key": 1, "date_key": 2024, "is_active_flag": 0},
        ]
        ranges = [("2024-01-01", "2025-01-01")]
        result = expand_pipeline_years(transformed, ranges, _key_fn)
        assert len(result) == 1

    def test_empty_input(self):
        result = expand_pipeline_years([], [], _key_fn)
        assert result == []

    def test_explicit_reporting_years(self):
        transformed = [
            {"candidate_key": 1, "date_key": 2020, "is_active_flag": 0},
        ]
        ranges = [("2020-01-01", "2025-01-01")]
        result = expand_pipeline_years(
            transformed, ranges, _key_fn,
            reporting_years={2020, 2022, 2024},
        )
        # Infills for 2022, 2024 (between 2020 and 2025)
        assert len(result) == 3

    def test_no_reporting_years(self):
        transformed = [{"candidate_key": 1}]
        ranges = [(None, None)]
        result = expand_pipeline_years(transformed, ranges, _key_fn)
        assert len(result) == 1
