"""Tests for bridge transformation logic."""

from unittest.mock import MagicMock

import pytest

from config import schema_map
from src import bridges as bridges_mod
from src.bridges import transform_bridge, transform_delimited_bridge, transform_union_bridge
from src.transformer import Transformer


class TestDelimitedBridge:
    """Test generalized delimited bridge transformation."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor and caches."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        # Set up candidate cache
        t._dim_caches["dim_candidate_core"] = {"cand-1": 1, "cand-2": 2}
        return t

    def test_delimited_bridge_developer(self, transformer):
        """Delimited bridge produces correct column name for developer_key."""
        transformer._dim_caches["dim_developer"] = {"Dev A": 10, "Dev B": 11}
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-1", "developersaggregated": "Dev A; Dev B"},
        ]

        config = {
            "_source_table": "vin_candidates",
            "_pk": None,
            "_special": {
                "bridge_from_delimited": True,
                "source_column": "developersaggregated",
                "delimiter": ";",
                "dimension_table": "dim_developer",
                "dimension_lookup_col": "developer_name",
            },
            "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
            "developer_key": "FK:dim_developer.developer_name|DELIMITED_VALUE",
        }
        special = config["_special"]

        result = transform_delimited_bridge(transformer, "bridge_candidate_developer", config, special)

        assert len(result) == 2
        assert result[0] == {"candidate_key": 1, "developer_key": 10}
        assert result[1] == {"candidate_key": 1, "developer_key": 11}

    def test_delimited_bridge_funder(self, transformer):
        """Delimited bridge produces correct column name for funder_key."""
        transformer._dim_caches["dim_funder"] = {"Funder X": 20, "Funder Y": 21}
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-2", "knownfundersaggregated": "Funder X; Funder Y"},
        ]

        config = {
            "_source_table": "vin_candidates",
            "_pk": None,
            "_special": {
                "bridge_from_delimited": True,
                "source_column": "knownfundersaggregated",
                "delimiter": ";",
                "dimension_table": "dim_funder",
                "dimension_lookup_col": "funder_name",
            },
            "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
            "funder_key": "FK:dim_funder.funder_name|DELIMITED_VALUE",
        }
        special = config["_special"]

        result = transform_delimited_bridge(transformer, "bridge_candidate_funder", config, special)

        assert len(result) == 2
        assert result[0] == {"candidate_key": 2, "funder_key": 20}
        assert result[1] == {"candidate_key": 2, "funder_key": 21}

    def test_delimited_bridge_skips_unknown_dim_values(self, transformer):
        """Delimited bridge skips values not found in dimension."""
        transformer._dim_caches["dim_funder"] = {"Funder X": 20}
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-1", "knownfundersaggregated": "Funder X; Unknown Funder"},
        ]

        config = {
            "_source_table": "vin_candidates",
            "_pk": None,
            "_special": {
                "bridge_from_delimited": True,
                "source_column": "knownfundersaggregated",
                "delimiter": ";",
                "dimension_table": "dim_funder",
                "dimension_lookup_col": "funder_name",
            },
            "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
            "funder_key": "FK:dim_funder.funder_name|DELIMITED_VALUE",
        }
        special = config["_special"]

        result = transform_delimited_bridge(transformer, "bridge_candidate_funder", config, special)

        assert len(result) == 1
        assert result[0] == {"candidate_key": 1, "funder_key": 20}


class TestStandardJunctionBridge:
    """Test standard junction bridge transformation."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor and caches."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        t._dim_caches["dim_candidate_core"] = {"cand-1": 1, "cand-2": 2}
        t._dim_caches["dim_age_group"] = {909670000: 10, 909670001: 11}
        return t

    def test_junction_bridge_with_option_code(self, transformer, monkeypatch):
        """Junction bridge resolves option_code FKs correctly."""
        transformer.extractor.extract_table.return_value = [
            {"entity_id": "cand-1", "option_code": 909670000},
            {"entity_id": "cand-1", "option_code": 909670001},
            {"entity_id": "cand-2", "option_code": 909670000},
        ]

        # Monkeypatch STAR_SCHEMA_MAP to include our test config
        test_map = dict(schema_map.STAR_SCHEMA_MAP)
        test_map["bridge_candidate_age_group"] = {
            "_source_table": "_junction_vin_candidates_new_agespecific",
            "_pk": None,
            "candidate_key": "FK:dim_candidate_core.candidateid|entity_id",
            "age_group_key": "FK:dim_age_group.option_code|option_code",
        }
        monkeypatch.setattr(schema_map, "STAR_SCHEMA_MAP", test_map)
        monkeypatch.setattr(bridges_mod, "STAR_SCHEMA_MAP", test_map)

        result = transform_bridge(transformer, "bridge_candidate_age_group")

        assert len(result) == 3
        assert result[0] == {"candidate_key": 1, "age_group_key": 10}
        assert result[1] == {"candidate_key": 1, "age_group_key": 11}
        assert result[2] == {"candidate_key": 2, "age_group_key": 10}

    def test_junction_bridge_filters_unresolved_fks(self, transformer, monkeypatch):
        """Junction bridge skips rows where FK cannot be resolved."""
        transformer.extractor.extract_table.return_value = [
            {"entity_id": "cand-1", "option_code": 909670000},
            {"entity_id": "unknown-cand", "option_code": 909670000},  # Unknown candidate
            {"entity_id": "cand-1", "option_code": 999999},  # Unknown option code
        ]

        test_map = dict(schema_map.STAR_SCHEMA_MAP)
        test_map["bridge_candidate_age_group"] = {
            "_source_table": "_junction_vin_candidates_new_agespecific",
            "_pk": None,
            "candidate_key": "FK:dim_candidate_core.candidateid|entity_id",
            "age_group_key": "FK:dim_age_group.option_code|option_code",
        }
        monkeypatch.setattr(schema_map, "STAR_SCHEMA_MAP", test_map)
        monkeypatch.setattr(bridges_mod, "STAR_SCHEMA_MAP", test_map)

        result = transform_bridge(transformer, "bridge_candidate_age_group")

        assert len(result) == 1
        assert result[0] == {"candidate_key": 1, "age_group_key": 10}


class TestTrialGeographyBridge:
    """Test trial geography bridge with fact-key caching."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with trial and geography caches."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        # Cache fact_clinical_trial_event by vin_clinicaltrialid
        t._dim_caches["fact_clinical_trial_event"] = {"trial-abc": 1, "trial-def": 2}
        t._dim_caches["dim_geography"] = {"country-001": 10, "country-002": 11}
        return t

    def test_trial_geography_bridge(self, transformer, monkeypatch):
        """Trial geography bridge resolves trial and country FKs."""
        transformer.extractor.extract_table.return_value = [
            {"vin_clinicaltrialid": "trial-abc", "vin_countryid": "country-001"},
            {"vin_clinicaltrialid": "trial-abc", "vin_countryid": "country-002"},
            {"vin_clinicaltrialid": "trial-def", "vin_countryid": "country-001"},
        ]

        test_map = dict(schema_map.STAR_SCHEMA_MAP)
        test_map["bridge_trial_geography"] = {
            "_source_table": "vin_vin_clinicaltrial_vin_countryset",
            "_pk": None,
            "_special": {"trial_bridge": True},
            "trial_key": "FK:fact_clinical_trial_event.clinicaltrialid|vin_clinicaltrialid",
            "country_key": "FK:dim_geography.vin_countryid|vin_countryid",
        }
        monkeypatch.setattr(schema_map, "STAR_SCHEMA_MAP", test_map)
        monkeypatch.setattr(bridges_mod, "STAR_SCHEMA_MAP", test_map)

        result = transform_bridge(transformer, "bridge_trial_geography")

        assert len(result) == 3
        assert result[0] == {"trial_key": 1, "country_key": 10}
        assert result[1] == {"trial_key": 1, "country_key": 11}
        assert result[2] == {"trial_key": 2, "country_key": 10}


class TestUnionBridgeDeveloperLocation:
    """Test Developer Location sourced from vin_developers via country_name_lookup."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor and caches."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        t._dim_caches["dim_candidate_core"] = {"cand-1": 1, "cand-2": 2}
        t._dim_caches["dim_geography_by_country_name"] = {"France": 10, "Turkey": 11}
        return t

    def _source_def(self):
        return {
            "table": "vin_developers",
            "candidate_col": "candidateid",
            "country_col": "country_name",
            "location_scope": "Developer Location",
            "country_name_lookup": True,
        }

    def _config(self):
        return {
            "_source_table": "UNION",
            "_pk": None,
            "_special": {"union_sources": [self._source_def()]},
            "candidate_key": "FK:dim_candidate_core.candidateid|candidate_col",
            "country_key": "FK:dim_geography.vin_countryid|country_col",
            "location_scope": "LITERAL:location_scope",
        }

    def test_valid_developer_row(self, transformer):
        """Valid developer row produces bridge row with Developer Location scope."""
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-1", "country_name": "France"},
        ]

        result = transform_union_bridge(
            transformer,
            "bridge_candidate_geography",
            self._config(),
            self._config()["_special"],
        )

        assert result == [
            {"candidate_key": 1, "country_key": 10, "location_scope": "Developer Location"},
        ]

    def test_null_country_name_skipped(self, transformer):
        """Row with country_name=None is skipped."""
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-1", "country_name": None},
        ]

        result = transform_union_bridge(
            transformer,
            "bridge_candidate_geography",
            self._config(),
            self._config()["_special"],
        )

        assert result == []

    def test_unknown_candidateid_skipped(self, transformer):
        """Row with unrecognised candidateid is skipped."""
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "unknown", "country_name": "France"},
        ]

        result = transform_union_bridge(
            transformer,
            "bridge_candidate_geography",
            self._config(),
            self._config()["_special"],
        )

        assert result == []

    def test_unknown_country_name_skipped(self, transformer):
        """Row with country_name not in dim_geography is skipped."""
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-1", "country_name": "Narnia"},
        ]

        result = transform_union_bridge(
            transformer,
            "bridge_candidate_geography",
            self._config(),
            self._config()["_special"],
        )

        assert result == []

    def test_duplicate_candidate_country_deduplicated(self, transformer):
        """Two developers in same country for same candidate produce one bridge row."""
        transformer.extractor.extract_table.return_value = [
            {"candidateid": "cand-1", "country_name": "France"},
            {"candidateid": "cand-1", "country_name": "France"},
        ]

        result = transform_union_bridge(
            transformer,
            "bridge_candidate_geography",
            self._config(),
            self._config()["_special"],
        )

        assert result == [
            {"candidate_key": 1, "country_key": 10, "location_scope": "Developer Location"},
        ]
