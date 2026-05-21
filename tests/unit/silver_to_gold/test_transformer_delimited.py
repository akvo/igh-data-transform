"""Unit tests for Transformer._transform_delimited_dimension."""

from unittest.mock import MagicMock

from igh_data_transform.transformations.silver_to_gold.core.transformer import (
    Transformer,
)


def _extractor_with_tables(tables: dict[str, list[dict]]) -> MagicMock:
    """Build a mock extractor whose extract_table(name, cols) yields the
    pre-canned rows for `name` (filtered to `cols` if requested)."""

    def extract_table(table_name, columns=None):
        rows = tables.get(table_name, [])
        if columns:
            yield from ({c: r.get(c) for c in columns} for r in rows)
        else:
            yield from rows

    ext = MagicMock()
    ext.extract_table.side_effect = extract_table
    return ext


class TestDelimitedDimensionWithoutEnrichment:
    """The pre-existing dim_funder-style behaviour must keep working."""

    def test_emits_one_row_per_distinct_value_sorted(self):
        ext = _extractor_with_tables(
            {
                "vin_candidates": [
                    {"developersaggregated": "Charlie Inc; Alpha Ltd"},
                    {"developersaggregated": "Bravo Co; Alpha Ltd"},
                    {"developersaggregated": None},
                ],
            }
        )
        transformer = Transformer(ext)
        config = {
            "_source_table": "vin_candidates",
            "_pk": "developer_key",
            "developer_name": "DELIMITED_VALUE",
        }
        special = {
            "extract_distinct_from_delimited": True,
            "source_column": "developersaggregated",
            "delimiter": ";",
        }

        rows = transformer._transform_delimited_dimension(
            "dim_developer", config, special
        )

        assert [r["developer_name"] for r in rows] == [
            "Alpha Ltd",
            "Bravo Co",
            "Charlie Inc",
        ]


class TestDelimitedDimensionWithEnrichment:
    """New: _special.enrich_from attaches columns from a secondary table."""

    def _build(self, candidates, developers):
        ext = _extractor_with_tables(
            {
                "vin_candidates": candidates,
                "vin_developers": developers,
            }
        )
        config = {
            "_source_table": "vin_candidates",
            "_pk": "developer_key",
            "developer_name": "DELIMITED_VALUE",
            "org_type": "ENRICHED",
        }
        special = {
            "extract_distinct_from_delimited": True,
            "source_column": "developersaggregated",
            "delimiter": ";",
            "enrich_from": {
                "table": "vin_developers",
                "match_target": "org_name",
                "attach": {"org_type": "org_type"},
            },
        }
        return Transformer(ext)._transform_delimited_dimension(
            "dim_developer", config, special
        )

    def test_attaches_matched_value(self):
        rows = self._build(
            candidates=[{"developersaggregated": "Sanaria Inc; Swiss TPH"}],
            developers=[
                {"org_name": "Sanaria Inc", "org_type": "For Profit SME"},
                {
                    "org_name": "Swiss TPH",
                    "org_type": "Academic and other research institutions",
                },
            ],
        )
        by_name = {r["developer_name"]: r["org_type"] for r in rows}
        assert by_name["Sanaria Inc"] == "For Profit SME"
        assert by_name["Swiss TPH"] == "Academic and other research institutions"

    def test_unmatched_name_gets_none(self):
        rows = self._build(
            candidates=[
                {
                    "developersaggregated": "Sanaria Inc; Free-Text-Only Co",
                }
            ],
            developers=[
                {"org_name": "Sanaria Inc", "org_type": "For Profit SME"},
            ],
        )
        by_name = {r["developer_name"]: r["org_type"] for r in rows}
        assert by_name["Sanaria Inc"] == "For Profit SME"
        assert by_name["Free-Text-Only Co"] is None

    def test_silver_null_org_type_propagates_as_none(self):
        # Silver's developers.py already nulls out numeric-code org_type
        # values. The transformer should pass that NULL straight through,
        # not invent a fallback.
        rows = self._build(
            candidates=[{"developersaggregated": "Mystery Org"}],
            developers=[
                {"org_name": "Mystery Org", "org_type": None},
            ],
        )
        assert rows == [
            {"developer_name": "Mystery Org", "org_type": None},
        ]

    def test_duplicate_org_name_first_wins(self):
        # vin_developers carries one row per (candidate, developer), so
        # the same org_name can appear many times. All occurrences share
        # the same accounts.vin_organisationtype, so the tie-break is
        # analytically irrelevant — just take the first seen.
        rows = self._build(
            candidates=[{"developersaggregated": "Repeating Org"}],
            developers=[
                {"org_name": "Repeating Org", "org_type": "For Profit SME"},
                {"org_name": "Repeating Org", "org_type": "For Profit SME"},
                {"org_name": "Repeating Org", "org_type": "For Profit SME"},
            ],
        )
        assert rows[0]["org_type"] == "For Profit SME"

    def test_duplicate_org_name_first_value_wins_over_later_different_value(self):
        # The "first wins" contract must hold even when later duplicate
        # rows carry different values — guards against an accidental
        # last-wins or pick-most-common refactor.
        rows = self._build(
            candidates=[{"developersaggregated": "Ambiguous Org"}],
            developers=[
                {"org_name": "Ambiguous Org", "org_type": "For Profit SME"},
                {"org_name": "Ambiguous Org", "org_type": "Public sector government"},
            ],
        )
        assert rows[0]["org_type"] == "For Profit SME"

    def test_null_match_target_row_in_secondary_table_is_skipped(self):
        # A vin_developers row with no org_name cannot contribute to the
        # lookup — skip it so the real "Known Org" match still wins.
        rows = self._build(
            candidates=[{"developersaggregated": "Known Org"}],
            developers=[
                {"org_name": None, "org_type": "Phantom"},
                {"org_name": "Known Org", "org_type": "For Profit SME"},
            ],
        )
        assert rows[0]["org_type"] == "For Profit SME"

    def test_empty_secondary_table_yields_all_none_org_type(self):
        # Realistic during a cold run before vin_developers is populated.
        rows = self._build(
            candidates=[{"developersaggregated": "Any Org"}],
            developers=[],
        )
        assert rows == [{"developer_name": "Any Org", "org_type": None}]


class TestDelimitedDimensionEnrichmentValidation:
    """Misconfigured enrich_from blocks should fail loudly, not silently."""

    def _run_with_enrich(self, enrich_from):
        ext = _extractor_with_tables(
            {
                "vin_candidates": [{"developersaggregated": "Any Org"}],
            }
        )
        config = {
            "_source_table": "vin_candidates",
            "_pk": "developer_key",
            "developer_name": "DELIMITED_VALUE",
            "org_type": "ENRICHED",
        }
        special = {
            "extract_distinct_from_delimited": True,
            "source_column": "developersaggregated",
            "delimiter": ";",
            "enrich_from": enrich_from,
        }
        return Transformer(ext)._transform_delimited_dimension(
            "dim_developer", config, special
        )

    def test_missing_table_key_raises(self):
        import pytest

        with pytest.raises(ValueError, match="missing keys"):
            self._run_with_enrich(
                {
                    "match_target": "org_name",
                    "attach": {"org_type": "org_type"},
                }
            )

    def test_missing_match_target_key_raises(self):
        import pytest

        with pytest.raises(ValueError, match="missing keys"):
            self._run_with_enrich(
                {
                    "table": "vin_developers",
                    "attach": {"org_type": "org_type"},
                }
            )

    def test_missing_attach_key_raises(self):
        import pytest

        with pytest.raises(ValueError, match="missing keys"):
            self._run_with_enrich(
                {
                    "table": "vin_developers",
                    "match_target": "org_name",
                }
            )
