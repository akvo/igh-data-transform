"""Tests for silver_to_gold dimension helpers and phase_sort_order config."""

from unittest.mock import MagicMock

from igh_data_transform.transformations.silver_to_gold.core.dimensions import (
    generate_date_dimension,
    postprocess_dim_phase,
)
from igh_data_transform.transformations.silver_to_gold.config.phase_sort_order import (
    PHASE_SORT_ORDER,
    collect_referenced_phase_names,
    inject_synthetic_phases,
)


# =========================================================================
# generate_date_dimension
# =========================================================================


class TestGenerateDateDimension:
    def test_default_range(self):
        rows = generate_date_dimension({"start_year": 2015, "end_year": 2030})
        years = {r["year"] for r in rows}
        assert years == set(range(2015, 2031))

    def test_single_year(self):
        rows = generate_date_dimension({"start_year": 2024, "end_year": 2024})
        years = {r["year"] for r in rows}
        assert years == {2024}
        # 2024 is a leap year: 366 days
        assert len(rows) == 366

    def test_row_structure(self):
        rows = generate_date_dimension({"start_year": 2025, "end_year": 2025})
        first = rows[0]
        assert first["full_date"] == "2025-01-01"
        assert first["year"] == 2025
        assert first["quarter"] == 1

    def test_quarters_correct(self):
        rows = generate_date_dimension({"start_year": 2025, "end_year": 2025})
        quarters = {r["quarter"] for r in rows}
        assert quarters == {1, 2, 3, 4}
        # Q1 starts Jan 1
        jan1 = next(r for r in rows if r["full_date"] == "2025-01-01")
        assert jan1["quarter"] == 1
        # Q2 starts Apr 1
        apr1 = next(r for r in rows if r["full_date"] == "2025-04-01")
        assert apr1["quarter"] == 2

    def test_defaults_when_missing(self):
        rows = generate_date_dimension({})
        years = {r["year"] for r in rows}
        assert 2015 in years
        assert 2030 in years


# =========================================================================
# collect_referenced_phase_names
# =========================================================================


class TestCollectReferencedPhaseNames:
    def _mock_extractor(self, rows):
        extractor = MagicMock()
        extractor.extract_table.return_value = iter(rows)
        return extractor

    def test_basic(self):
        ext = self._mock_extractor([
            {"new_currentrdstage": "Phase I"},
            {"new_currentrdstage": "Phase II"},
        ])
        result = collect_referenced_phase_names(ext)
        assert result == {"Phase I", "Phase II"}

    def test_strips_product_suffix(self):
        ext = self._mock_extractor([
            {"new_currentrdstage": "Phase III - Drugs"},
        ])
        result = collect_referenced_phase_names(ext)
        assert result == {"Phase III"}

    def test_applies_aliases(self):
        ext = self._mock_extractor([
            {"new_currentrdstage": "Approved product"},
        ])
        result = collect_referenced_phase_names(ext)
        assert result == {"Approved"}

    def test_skips_empty(self):
        ext = self._mock_extractor([
            {"new_currentrdstage": None},
            {"new_currentrdstage": ""},
        ])
        result = collect_referenced_phase_names(ext)
        assert result == set()

    def test_deduplicates(self):
        ext = self._mock_extractor([
            {"new_currentrdstage": "Phase I"},
            {"new_currentrdstage": "Phase I"},
        ])
        result = collect_referenced_phase_names(ext)
        assert result == {"Phase I"}


# =========================================================================
# inject_synthetic_phases
# =========================================================================


class TestInjectSyntheticPhases:
    def test_adds_missing_phases(self):
        existing = [{"phase_name": "Phase I", "sort_order": 30}]
        # All PHASE_SORT_ORDER phases are referenced
        referenced = set(PHASE_SORT_ORDER.keys())
        result = inject_synthetic_phases(existing, referenced)
        names = {r["phase_name"] for r in result}
        for phase in PHASE_SORT_ORDER:
            assert phase in names

    def test_does_not_duplicate_existing(self):
        existing = [{"phase_name": "Phase I", "sort_order": 30}]
        referenced = {"Phase I", "Phase II"}
        result = inject_synthetic_phases(existing, referenced)
        phase_i_count = sum(1 for r in result if r["phase_name"] == "Phase I")
        assert phase_i_count == 1

    def test_filters_by_referenced(self):
        existing = []
        referenced = {"Phase I"}
        result = inject_synthetic_phases(existing, referenced)
        names = {r["phase_name"] for r in result}
        assert "Phase I" in names
        # Phases not in referenced should not be injected
        assert "Phase III" not in names

    def test_no_filter_when_none(self):
        existing = []
        result = inject_synthetic_phases(existing, None)
        names = {r["phase_name"] for r in result}
        for phase in PHASE_SORT_ORDER:
            assert phase in names


# =========================================================================
# postprocess_dim_phase
# =========================================================================


class TestPostprocessDimPhase:
    def test_deduplicates_by_name(self):
        extractor = MagicMock()
        extractor.extract_table.return_value = iter([
            {"new_currentrdstage": "Phase I"},
        ])
        transformed = [
            {"phase_name": "Phase I", "sort_order": 30},
            {"phase_name": "Phase I", "sort_order": 30},  # duplicate
        ]
        result = postprocess_dim_phase(transformed, extractor)
        phase_i_count = sum(1 for r in result if r["phase_name"] == "Phase I")
        assert phase_i_count == 1

    def test_filters_unreferenced(self):
        extractor = MagicMock()
        extractor.extract_table.return_value = iter([
            {"new_currentrdstage": "Phase I"},
        ])
        transformed = [
            {"phase_name": "Phase I", "sort_order": 30},
            {"phase_name": "Phase II", "sort_order": 40},
        ]
        result = postprocess_dim_phase(transformed, extractor)
        names = {r["phase_name"] for r in result}
        assert "Phase I" in names
        assert "Phase II" not in names
