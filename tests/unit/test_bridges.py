"""Tests for bridge transformation functions."""

from unittest.mock import MagicMock

import pytest

from igh_data_transform.transformations.silver_to_gold.core import bridges


class TestBridgeCandidatePriorityFiltering:
    """bridge_candidate_priority excludes non-pipeline candidates."""

    SCHEMA_MAP = {
        "bridge_candidate_priority": {
            "_source_table": "vin_rdpriorities",
            "_special": {},
            "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
            "priority_key": "FK:dim_priority.rdpriorityid|rdpriorityid",
        }
    }

    def _make_transformer(
        self,
        source_rows: list[dict],
        pipeline_candidate_keys: set[int],
        dim_caches: dict | None = None,
    ) -> MagicMock:
        transformer = MagicMock()
        transformer.extractor.extract_table.return_value = source_rows
        transformer.get_pipeline_candidate_keys.return_value = pipeline_candidate_keys

        if dim_caches:
            transformer.lookup_dimension_key.side_effect = (
                lambda table, val: dim_caches.get(table, {}).get(val)
            )
        else:
            transformer.lookup_dimension_key.side_effect = lambda _t, v: v

        transformer._evaluate_expression.side_effect = lambda expr, row: row.get(expr)
        return transformer

    def test_filters_non_pipeline_candidates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(bridges, "STAR_SCHEMA_MAP", self.SCHEMA_MAP)

        source_rows = [
            {"candidateid": "c1", "rdpriorityid": "p1"},
            {"candidateid": "c2", "rdpriorityid": "p2"},
            {"candidateid": "c3", "rdpriorityid": "p3"},
        ]
        dim_caches = {
            "dim_candidate_core": {"c1": 1, "c2": 2, "c3": 3},
            "dim_priority": {"p1": 10, "p2": 20, "p3": 30},
        }
        # Only candidate keys 1 and 3 are in pipeline
        pipeline_keys = {1, 3}

        transformer = self._make_transformer(source_rows, pipeline_keys, dim_caches)
        result = bridges.transform_bridge(transformer, "bridge_candidate_priority")

        assert len(result) == 2
        assert {r["candidate_key"] for r in result} == {1, 3}

    def test_empty_pipeline_keys_filters_all(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(bridges, "STAR_SCHEMA_MAP", self.SCHEMA_MAP)

        source_rows = [
            {"candidateid": "c1", "rdpriorityid": "p1"},
        ]
        dim_caches = {
            "dim_candidate_core": {"c1": 1},
            "dim_priority": {"p1": 10},
        }
        transformer = self._make_transformer(source_rows, set(), dim_caches)
        result = bridges.transform_bridge(transformer, "bridge_candidate_priority")

        assert len(result) == 0
