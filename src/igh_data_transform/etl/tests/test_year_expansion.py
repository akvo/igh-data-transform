"""Tests for pipeline snapshot year expansion logic."""

import pytest

from src.year_expansion import expand_pipeline_years

# The full set of reporting years in the IGH SCD2 data
REPORTING_YEARS = {2019, 2021, 2023, 2024, 2025}

# dim_date surrogate key lookup used in tests
DATE_KEYS = {
    "2019-01-01": 100,
    "2021-01-01": 200,
    "2023-01-01": 300,
    "2024-01-01": 400,
    "2025-01-01": 500,
}


@pytest.fixture
def lookup_date_key():
    """Return a date-key lookup callable backed by DATE_KEYS."""
    return DATE_KEYS.get


def _make_row(date_key, is_active_flag=0, phase_key=10, include_in_pipeline="Yes"):
    """Helper to build a fact row."""
    return {
        "candidate_key": 1,
        "product_key": 2,
        "disease_key": 3,
        "phase_key": phase_key,
        "date_key": date_key,
        "is_active_flag": is_active_flag,
        "include_in_pipeline": include_in_pipeline,
    }


class TestExpandPipelineYears:
    """Test expand_pipeline_years function."""

    def test_spanning_record_creates_infill(self, lookup_date_key):
        """Record valid 2023→2025 creates a 2024 carry-forward row."""
        row = _make_row(date_key=300, is_active_flag=0)
        ranges = [("2023-01-01T00:00:00Z", "2025-01-01T00:00:00Z")]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        assert len(result) == 2
        assert result[0]["date_key"] == 300
        assert result[0]["is_active_flag"] == 0
        assert result[1]["date_key"] == 400
        assert result[1]["is_active_flag"] == 0

    def test_active_record_infills_with_active_at_latest(self, lookup_date_key):
        """Active record (valid_to=None) from 2023 infills 2024 and 2025."""
        row = _make_row(date_key=300, is_active_flag=1)
        ranges = [("2023-01-01T00:00:00Z", None)]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        assert len(result) == 3
        assert result[0]["date_key"] == 300
        assert result[0]["is_active_flag"] == 0
        assert result[1]["date_key"] == 400
        assert result[1]["is_active_flag"] == 0
        assert result[2]["date_key"] == 500
        assert result[2]["is_active_flag"] == 1

    def test_consecutive_boundaries_no_expansion(self, lookup_date_key):
        """Record valid 2023→2024 has no intermediate years to fill."""
        row = _make_row(date_key=300, is_active_flag=0)
        ranges = [("2023-01-01T00:00:00Z", "2024-01-01T00:00:00Z")]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        assert len(result) == 1
        assert result[0] is row

    def test_last_reporting_year_no_expansion(self, lookup_date_key):
        """Record at 2025 (max reporting year) with no valid_to creates no infill."""
        row = _make_row(date_key=500, is_active_flag=1)
        ranges = [("2025-01-01T00:00:00Z", None)]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        assert len(result) == 1
        assert result[0]["date_key"] == 500
        assert result[0]["is_active_flag"] == 1

    def test_carryforward_inherits_all_columns(self, lookup_date_key):
        """Infill rows copy all columns from original except date_key and is_active_flag."""
        row = _make_row(date_key=300, is_active_flag=0, phase_key=42, include_in_pipeline="Yes")
        row["candidate_key"] = 99
        row["product_key"] = 88
        row["disease_key"] = 77
        ranges = [("2023-01-01T00:00:00Z", "2025-01-01T00:00:00Z")]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        infill = result[1]
        assert infill["candidate_key"] == 99
        assert infill["product_key"] == 88
        assert infill["disease_key"] == 77
        assert infill["phase_key"] == 42
        assert infill["include_in_pipeline"] == "Yes"
        assert infill["date_key"] == 400

    def test_multiple_records_mixed(self, lookup_date_key):
        """Multiple records: one spanning, one not, one active."""
        row_spanning = _make_row(date_key=300, is_active_flag=0)
        row_spanning["candidate_key"] = 1
        row_no_span = _make_row(date_key=400, is_active_flag=0)
        row_no_span["candidate_key"] = 2
        row_active = _make_row(date_key=300, is_active_flag=1)
        row_active["candidate_key"] = 3

        ranges = [
            ("2023-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            ("2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            ("2023-01-01T00:00:00Z", None),
        ]

        result = expand_pipeline_years(
            [row_spanning, row_no_span, row_active],
            ranges,
            lookup_date_key,
            reporting_years=REPORTING_YEARS,
        )

        assert len(result) == 6

        active_rows = [r for r in result if r["candidate_key"] == 3]
        assert len(active_rows) == 3
        assert active_rows[0]["is_active_flag"] == 0
        assert active_rows[1]["is_active_flag"] == 0
        assert active_rows[2]["is_active_flag"] == 1

    def test_non_active_record_no_flag_flip(self, lookup_date_key):
        """Non-active record (valid_to set) keeps original is_active_flag=0."""
        row = _make_row(date_key=300, is_active_flag=0)
        ranges = [("2023-01-01T00:00:00Z", "2025-06-15T00:00:00Z")]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        assert result[0]["is_active_flag"] == 0
        assert result[1]["is_active_flag"] == 0

    def test_empty_input(self, lookup_date_key):
        """Empty input returns empty output."""
        result = expand_pipeline_years([], [], lookup_date_key)
        assert result == []

    def test_gap_2019_to_2023(self, lookup_date_key):
        """Record from 2019→2023 infills 2021 (the only reporting year between)."""
        row = _make_row(date_key=100, is_active_flag=0)
        ranges = [("2019-01-01T00:00:00Z", "2023-01-01T00:00:00Z")]

        result = expand_pipeline_years([row], ranges, lookup_date_key, reporting_years=REPORTING_YEARS)

        assert len(result) == 2
        assert result[0]["date_key"] == 100
        assert result[1]["date_key"] == 200

    def test_auto_detects_reporting_years(self, lookup_date_key):
        """Without explicit reporting_years, years are auto-detected from ranges."""
        rows = [
            _make_row(date_key=300, is_active_flag=0),
            _make_row(date_key=400, is_active_flag=0),
            _make_row(date_key=500, is_active_flag=0),
        ]
        rows[0]["candidate_key"] = 1
        rows[1]["candidate_key"] = 2
        rows[2]["candidate_key"] = 3

        ranges = [
            ("2023-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            ("2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            ("2025-01-01T00:00:00Z", None),
        ]

        result = expand_pipeline_years(rows, ranges, lookup_date_key)

        assert len(result) == 4
        assert result[1]["date_key"] == 400
