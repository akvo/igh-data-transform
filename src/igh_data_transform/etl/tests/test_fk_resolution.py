"""Tests for FK resolution on transformer (dimension FK lookups and cross-references)."""

from unittest.mock import MagicMock

import pytest

from src.transformer import Transformer


class TestDimensionFKLookups:
    """Test FK resolution on dimension tables (e.g. dim_priority.disease_key)."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with dim_disease cache pre-populated."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        # Simulate dim_disease already loaded and cached
        t._dim_caches["dim_disease"] = {
            "disease-guid-1": 10,
            "disease-guid-2": 20,
        }
        # Mock extraction of vin_rdpriorities (silver column names)
        mock_extractor.extract_table.return_value = [
            {
                "rdpriorityid": "priority-1",
                "name": "Priority A",
                "indication": "Malaria",
                "intendeduse": "Treatment",
                "diseasevalue": "disease-guid-1",
            },
            {
                "rdpriorityid": "priority-2",
                "name": "Priority B",
                "indication": None,
                "intendeduse": None,
                "diseasevalue": "disease-guid-2",
            },
            {
                "rdpriorityid": "priority-3",
                "name": "Priority C",
                "indication": None,
                "intendeduse": None,
                "diseasevalue": "unknown-guid",
            },
        ]
        return t

    def test_dimension_fk_resolves_disease_key(self, transformer):
        """dim_priority with fk_lookups resolves disease_key from cache."""
        result = transformer.transform_dimension("dim_priority")
        assert len(result) == 3
        assert result[0]["disease_key"] == 10
        assert result[1]["disease_key"] == 20

    def test_dimension_fk_returns_none_for_unknown(self, transformer):
        """dim_priority FK returns None when disease GUID not in cache."""
        result = transformer.transform_dimension("dim_priority")
        assert result[2]["disease_key"] is None

    def test_dimension_fk_preserves_other_columns(self, transformer):
        """dim_priority FK resolution doesn't affect other columns."""
        result = transformer.transform_dimension("dim_priority")
        assert result[0]["priority_name"] == "Priority A"
        assert result[0]["indication"] == "Malaria"
        assert not result[1]["intended_use"]


class TestCandidateCrossRef:
    """Test FK_VIA_CANDIDATE cross-reference resolution."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with candidate cross-ref maps."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        # Simulate loaded pipeline data
        pipeline_data = [
            {"candidate_key": 1, "disease_key": 10, "product_key": 20},
            {"candidate_key": 2, "disease_key": 11, "product_key": 21},
            {"candidate_key": 3, "disease_key": None, "product_key": 22},
        ]
        t.build_candidate_cross_refs(pipeline_data)
        return t

    def test_resolve_disease_key(self, transformer):
        """FK_VIA_CANDIDATE resolves disease_key from candidate."""
        new_row = {"candidate_key": 1}
        result = transformer._resolve_fk_via_candidate("FK_VIA_CANDIDATE:disease_key", new_row)
        assert result == 10

    def test_resolve_product_key(self, transformer):
        """FK_VIA_CANDIDATE resolves product_key from candidate."""
        new_row = {"candidate_key": 2}
        result = transformer._resolve_fk_via_candidate("FK_VIA_CANDIDATE:product_key", new_row)
        assert result == 21

    def test_resolve_missing_candidate(self, transformer):
        """FK_VIA_CANDIDATE returns None for unknown candidate."""
        new_row = {"candidate_key": 999}
        result = transformer._resolve_fk_via_candidate("FK_VIA_CANDIDATE:disease_key", new_row)
        assert result is None

    def test_resolve_null_candidate_key(self, transformer):
        """FK_VIA_CANDIDATE returns None when candidate_key is None."""
        new_row = {"candidate_key": None}
        result = transformer._resolve_fk_via_candidate("FK_VIA_CANDIDATE:disease_key", new_row)
        assert result is None

    def test_resolve_null_value_in_cross_ref(self, transformer):
        """FK_VIA_CANDIDATE returns None when cross-ref value is None."""
        new_row = {"candidate_key": 3}
        result = transformer._resolve_fk_via_candidate("FK_VIA_CANDIDATE:disease_key", new_row)
        assert result is None


class TestCompositeFKResolution:
    """Test composite FK resolution using dimension config expressions."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with tech dimension composite cache."""
        mock_extractor = MagicMock()
        mock_extractor.lookup_optionset = MagicMock(return_value=None)
        t = Transformer(mock_extractor)
        # Pre-populate composite cache for dim_candidate_tech
        t._composite_caches["dim_candidate_tech"] = {
            ("Vaccine",): 1,
            ("Drug",): 2,
            ("Unknown",): 3,
        }
        return t

    def test_composite_fk_resolves_from_dimension_config(self, transformer):
        """COMPOSITE FK evaluates dim's own expressions against the source row."""
        row = {"technologytype": "Vaccine"}
        fk_expr = "FK:dim_candidate_tech.COMPOSITE"
        result = transformer._resolve_fk(fk_expr, row, "")
        assert result == 1

    def test_composite_fk_coalesce_defaults(self, transformer):
        """COMPOSITE FK applies COALESCE default when source column is None."""
        row = {"technologytype": None}
        fk_expr = "FK:dim_candidate_tech.COMPOSITE"
        result = transformer._resolve_fk(fk_expr, row, "")
        assert result == 3  # COALESCE(technologytype, 'Unknown') -> 'Unknown'

    def test_composite_fk_returns_none_for_unknown_combo(self, transformer):
        """COMPOSITE FK returns None when key combo not in cache."""
        row = {"technologytype": "NovelType"}
        fk_expr = "FK:dim_candidate_tech.COMPOSITE"
        result = transformer._resolve_fk(fk_expr, row, "")
        assert result is None
