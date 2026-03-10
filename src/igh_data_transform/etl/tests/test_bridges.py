"""Tests for bridge transformation logic."""

from unittest.mock import MagicMock

import pytest

from config import schema_map
from src import bridges as bridges_mod
from src.bridges import (
    collect_structured_trial_geography_rows,
    collect_trial_geography_rows,
    parse_trial_locations,
    transform_bridge,
    transform_delimited_bridge,
    transform_union_bridge,
)
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
    """Test trial geography UNION bridge (structured + free-text sources)."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with trial and geography caches."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        t._dim_caches["fact_clinical_trial_event"] = {"trial-abc": 1, "trial-def": 2}
        t._dim_caches["dim_geography"] = {"country-001": 10, "country-002": 11}
        t._dim_caches["dim_geography_by_country_name"] = {"India": 10, "Egypt": 11}
        return t

    def test_structured_source_resolves_fks(self, transformer):
        """Structured junction table source resolves trial and country FKs."""
        source_def = {
            "table": "vin_vin_clinicaltrial_vin_countryset",
            "trial_col": "vin_clinicaltrialid",
            "country_col": "vin_countryid",
            "structured": True,
        }
        transformer.extractor.extract_table.return_value = [
            {"vin_clinicaltrialid": "trial-abc", "vin_countryid": "country-001"},
            {"vin_clinicaltrialid": "trial-def", "vin_countryid": "country-002"},
        ]

        result = collect_structured_trial_geography_rows(transformer, source_def)

        assert len(result) == 2
        assert result[0] == {"trial_key": 1, "country_key": 10}
        assert result[1] == {"trial_key": 2, "country_key": 11}

    def test_free_text_source_parses_locations(self, transformer):
        """Free-text locations source parses countries and maps to trial_key."""
        source_def = {
            "table": "vin_clinicaltrials",
            "trial_col": "clinicaltrialid",
            "country_col": "locations",
            "parse_trial_locations": True,
        }
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-abc", "locations": "India|Egypt"},
        ]

        result = collect_trial_geography_rows(transformer, source_def)

        assert len(result) == 2
        assert {"trial_key": 1, "country_key": 10} in result
        assert {"trial_key": 1, "country_key": 11} in result

    def test_union_deduplicates_across_sources(self, transformer):
        """Same trial+country from structured and free-text is deduplicated."""

        def mock_extract(table):
            if table == "vin_vin_clinicaltrial_vin_countryset":
                return [{"vin_clinicaltrialid": "trial-abc", "vin_countryid": "country-001"}]
            if table == "vin_clinicaltrials":
                return [{"clinicaltrialid": "trial-abc", "locations": "India"}]
            return []

        transformer.extractor.extract_table.side_effect = mock_extract

        config = {}
        special = {
            "trial_bridge": True,
            "union_sources": [
                {
                    "table": "vin_vin_clinicaltrial_vin_countryset",
                    "trial_col": "vin_clinicaltrialid",
                    "country_col": "vin_countryid",
                    "structured": True,
                },
                {
                    "table": "vin_clinicaltrials",
                    "trial_col": "clinicaltrialid",
                    "country_col": "locations",
                    "parse_trial_locations": True,
                },
            ],
        }

        result = transform_union_bridge(transformer, "bridge_trial_geography", config, special)

        # country-001 -> key 10, India -> key 10: same pair, should be deduplicated
        assert len(result) == 1
        assert result[0] == {"trial_key": 1, "country_key": 10}

    def test_free_text_skips_null_locations(self, transformer):
        """Trials with NULL locations produce no geography rows."""
        source_def = {
            "table": "vin_clinicaltrials",
            "trial_col": "clinicaltrialid",
            "country_col": "locations",
            "parse_trial_locations": True,
        }
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-abc", "locations": None},
        ]

        result = collect_trial_geography_rows(transformer, source_def)
        assert result == []

    def test_free_text_skips_unresolvable_trials(self, transformer):
        """Trials not in the fact cache are skipped."""
        source_def = {
            "table": "vin_clinicaltrials",
            "trial_col": "clinicaltrialid",
            "country_col": "locations",
            "parse_trial_locations": True,
        }
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "unknown-trial", "locations": "India"},
        ]

        result = collect_trial_geography_rows(transformer, source_def)
        assert result == []


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


# ---------------------------------------------------------------------------
# Trial location parsing (unit tests for parse_trial_locations helper)
# ---------------------------------------------------------------------------

# Minimal country cache matching dim_geography for testing
_TRIAL_COUNTRY_CACHE = {
    "India": 1,
    "Egypt": 2,
    "United States of America": 3,
    "United Kingdom": 4,
    "Thailand": 5,
    "Kuwait": 6,
    "Kazakhstan": 7,
    "Canada": 8,
    "Colombia": 9,
    "France": 10,
}


class TestParseTrialLocations:
    """Unit tests for the parse_trial_locations helper."""

    def test_simple_country(self):
        result = parse_trial_locations("India", _TRIAL_COUNTRY_CACHE)
        assert result == ["India"]

    def test_alias_resolution(self):
        result = parse_trial_locations("USA", _TRIAL_COUNTRY_CACHE)
        assert result == ["United States of America"]

    def test_case_insensitive(self):
        result = parse_trial_locations("thailand", _TRIAL_COUNTRY_CACHE)
        assert result == ["Thailand"]

    def test_pipe_delimited(self):
        result = parse_trial_locations("India|Egypt", _TRIAL_COUNTRY_CACHE)
        assert result == ["India", "Egypt"]

    def test_semicolon_delimited(self):
        result = parse_trial_locations("Kuwait;Kazakhstan", _TRIAL_COUNTRY_CACHE)
        assert result == ["Kuwait", "Kazakhstan"]

    def test_address_extraction(self):
        result = parse_trial_locations(
            "Mount Sinai Hospital, Toronto, Ontario, Canada",
            _TRIAL_COUNTRY_CACHE,
        )
        assert result == ["Canada"]

    def test_unknown_skipped(self):
        result = parse_trial_locations("Unknown", _TRIAL_COUNTRY_CACHE)
        assert result == []

    def test_empty_string(self):
        result = parse_trial_locations("", _TRIAL_COUNTRY_CACHE)
        assert result == []

    def test_dedup_within_text(self):
        result = parse_trial_locations("India|India", _TRIAL_COUNTRY_CACHE)
        assert result == ["India"]

    def test_parenthetical_country(self):
        result = parse_trial_locations("Hôpital Necker (France)", _TRIAL_COUNTRY_CACHE)
        assert result == ["France"]

    def test_alias_columbia_typo(self):
        """'Columbia' (common misspelling) resolves to 'Colombia'."""
        result = parse_trial_locations("Columbia", _TRIAL_COUNTRY_CACHE)
        assert result == ["Colombia"]


class TestTrialGeographyFreeTextParsing:
    """Test free-text trial location parsing via bridge_trial_geography UNION."""

    @pytest.fixture
    def transformer(self):
        """Create transformer with mocked extractor and caches."""
        mock_extractor = MagicMock()
        t = Transformer(mock_extractor)
        t._dim_caches["fact_clinical_trial_event"] = {"trial-1": 1, "trial-2": 2}
        t._dim_caches["dim_geography_by_country_name"] = {
            "India": 10,
            "Egypt": 11,
            "United States of America": 12,
            "Thailand": 13,
            "Canada": 14,
        }
        return t

    def _source_def(self):
        return {
            "table": "vin_clinicaltrials",
            "trial_col": "clinicaltrialid",
            "country_col": "locations",
            "parse_trial_locations": True,
        }

    def test_simple_country_name(self, transformer):
        """Simple country name produces one bridge row with trial_key."""
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-1", "locations": "India"},
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert result == [{"trial_key": 1, "country_key": 10}]

    def test_alias_resolution(self, transformer):
        """'USA' resolves to 'United States of America'."""
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-1", "locations": "USA"},
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert result == [{"trial_key": 1, "country_key": 12}]

    def test_case_insensitive(self, transformer):
        """Case-insensitive matching works."""
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-1", "locations": "thailand"},
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert result == [{"trial_key": 1, "country_key": 13}]

    def test_pipe_delimited(self, transformer):
        """Pipe-delimited locations produce multiple entries."""
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-1", "locations": "India|Egypt"},
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert len(result) == 2
        assert {"trial_key": 1, "country_key": 10} in result
        assert {"trial_key": 1, "country_key": 11} in result

    def test_address_extraction(self, transformer):
        """Address-like string extracts country from last comma-separated part."""
        transformer.extractor.extract_table.return_value = [
            {
                "clinicaltrialid": "trial-1",
                "locations": "Mount Sinai Hospital, Toronto, Ontario, Canada",
            },
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert result == [{"trial_key": 1, "country_key": 14}]

    def test_unknown_skipped(self, transformer):
        """'Unknown' locations produce no entries."""
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-1", "locations": "Unknown"},
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert result == []

    def test_null_locations_skipped(self, transformer):
        """None locations produce no entries."""
        transformer.extractor.extract_table.return_value = [
            {"clinicaltrialid": "trial-1", "locations": None},
        ]
        result = collect_trial_geography_rows(transformer, self._source_def())
        assert result == []
