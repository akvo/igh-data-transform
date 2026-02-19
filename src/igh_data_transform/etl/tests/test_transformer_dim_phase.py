"""Tests for dim_phase filtering: deduplication, unreferenced removal, synthetic injection."""

from unittest.mock import MagicMock

from config.phase_sort_order import (
    PHASE_SORT_ORDER,
    collect_referenced_phase_names,
    inject_synthetic_phases,
)
from src.transformer import Transformer


def _make_transformer(rdstage_rows, candidate_rows):
    """Build a Transformer whose extractor yields the given rows."""
    mock_extractor = MagicMock()

    def extract_table(table, columns=None):
        if table == "vin_rdstages":
            return iter(rdstage_rows)
        if table == "vin_candidates":
            return iter(candidate_rows)
        return iter([])

    mock_extractor.extract_table = MagicMock(side_effect=extract_table)
    return Transformer(mock_extractor)


class TestCollectReferencedPhaseNames:
    """Test collect_referenced_phase_names parsing logic."""

    def test_extracts_phase_before_dash(self):
        """Phase name is the text before ' - '."""
        t = _make_transformer(
            [],
            [
                {"new_currentrdstage": "Phase I - Drugs"},
                {"new_currentrdstage": "Phase II - Vaccines"},
            ],
        )
        assert collect_referenced_phase_names(t.extractor) == {"Phase I", "Phase II"}

    def test_uses_full_value_when_no_dash(self):
        """When there is no ' - ' separator, the full value is the phase name."""
        t = _make_transformer(
            [],
            [
                {"new_currentrdstage": "Approved"},
            ],
        )
        assert collect_referenced_phase_names(t.extractor) == {"Approved"}

    def test_applies_alias_normalization(self):
        """Alias like 'N/A' maps to 'Not applicable'."""
        t = _make_transformer(
            [],
            [
                {"new_currentrdstage": "N/A"},
            ],
        )
        result = collect_referenced_phase_names(t.extractor)
        assert "Not applicable" in result
        assert "N/A" not in result

    def test_skips_null_and_empty(self):
        """Null/empty values produce no entries."""
        t = _make_transformer(
            [],
            [
                {"new_currentrdstage": None},
                {"new_currentrdstage": ""},
                {"new_currentrdstage": "Phase I - Drugs"},
            ],
        )
        assert collect_referenced_phase_names(t.extractor) == {"Phase I"}


class TestDimPhaseFiltering:
    """Test the full dim_phase pipeline: dedup, filter, synthetic injection."""

    def test_unreferenced_phases_excluded(self):
        """Phases in vin_rdstages but never referenced by candidates are excluded."""
        rdstages = [
            {"vin_rdstageid": "id-a", "vin_name": "Phase I"},
            {"vin_rdstageid": "id-b", "vin_name": "Phase II"},
            {"vin_rdstageid": "id-c", "vin_name": "Clinical evaluation"},
        ]
        candidates = [
            {"new_currentrdstage": "Phase I - Drugs"},
            {"new_currentrdstage": "Phase II - Vaccines"},
        ]
        t = _make_transformer(rdstages, candidates)
        result = t.transform_dimension("dim_phase")
        names = {r["phase_name"] for r in result}
        assert "Phase I" in names
        assert "Phase II" in names
        assert "Clinical evaluation" not in names

    def test_duplicate_phase_names_deduplicated(self):
        """Two rows with the same phase_name only produce one output row."""
        rdstages = [
            {"vin_rdstageid": "id-1", "vin_name": "Not applicable"},
            {"vin_rdstageid": "id-2", "vin_name": "Not applicable"},
        ]
        candidates = [
            {"new_currentrdstage": "Not applicable"},
        ]
        t = _make_transformer(rdstages, candidates)
        result = t.transform_dimension("dim_phase")
        na_rows = [r for r in result if r["phase_name"] == "Not applicable"]
        assert len(na_rows) == 1
        # Should keep the first occurrence
        assert na_rows[0]["vin_rdstageid"] == "id-1"

    def test_synthetic_phase_injected_when_referenced(self):
        """A phase in PHASE_SORT_ORDER but not in vin_rdstages is injected when referenced."""
        rdstages = [
            {"vin_rdstageid": "id-a", "vin_name": "Phase I"},
        ]
        candidates = [
            {"new_currentrdstage": "Phase I - Drugs"},
            {"new_currentrdstage": "Approved"},  # In PHASE_SORT_ORDER but not in rdstages
        ]
        t = _make_transformer(rdstages, candidates)
        result = t.transform_dimension("dim_phase")
        names = {r["phase_name"] for r in result}
        assert "Approved" in names
        # Synthetic row should have None vin_rdstageid
        approved_row = next(r for r in result if r["phase_name"] == "Approved")
        assert approved_row["vin_rdstageid"] is None

    def test_synthetic_phase_not_injected_when_unreferenced(self):
        """A phase in PHASE_SORT_ORDER that no candidate references is not injected."""
        rdstages = [
            {"vin_rdstageid": "id-a", "vin_name": "Phase I"},
        ]
        candidates = [
            {"new_currentrdstage": "Phase I - Drugs"},
        ]
        t = _make_transformer(rdstages, candidates)
        result = t.transform_dimension("dim_phase")
        names = {r["phase_name"] for r in result}
        # "Post-marketing surveillance" is in PHASE_SORT_ORDER but not referenced
        assert "Post-marketing surveillance" not in names

    def test_alias_maps_approved_product_to_approved(self):
        """'Approved product' in candidates references the 'Approved' phase."""
        rdstages = [
            {"vin_rdstageid": "id-a", "vin_name": "Approved"},
        ]
        candidates = [
            {"new_currentrdstage": "Approved product - Vaccines"},
        ]
        t = _make_transformer(rdstages, candidates)
        result = t.transform_dimension("dim_phase")
        names = {r["phase_name"] for r in result}
        assert "Approved" in names


class TestInjectSyntheticPhases:
    """Test inject_synthetic_phases in isolation."""

    def test_without_referenced_filter_injects_all_missing(self):
        """When referenced_phases is None, all missing PHASE_SORT_ORDER entries are injected."""
        existing = [{"phase_name": "Phase I", "sort_order": 40}]
        result = inject_synthetic_phases(existing, referenced_phases=None)
        names = {r["phase_name"] for r in result}
        # Should have Phase I plus all others from PHASE_SORT_ORDER
        for phase in PHASE_SORT_ORDER:
            assert phase in names

    def test_with_referenced_filter_only_injects_referenced(self):
        """When referenced_phases is provided, only those missing phases are injected."""
        existing = [{"phase_name": "Phase I", "sort_order": 40}]
        referenced = {"Phase I", "Phase II", "Approved"}
        result = inject_synthetic_phases(existing, referenced_phases=referenced)
        names = {r["phase_name"] for r in result}
        assert "Phase I" in names  # Already existed
        assert "Phase II" in names  # Injected (referenced)
        assert "Approved" in names  # Injected (referenced)
        assert "Late development" not in names  # Not referenced
