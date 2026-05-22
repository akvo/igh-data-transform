"""Unit tests for delimited-bridge transformation.

The silver-layer ``vin_candidates`` table is intentionally fanned out
into one row per (candidate, R&D-stage/pipeline year boundary) by
:func:`igh_data_transform.transformations.candidates._expand_temporal_rows`.
Bridge tables built from that source by iterating per-row would emit
the same (candidate_key, dim_key) pair once per SCD2 version, even
though the candidate-to-developer (and candidate-to-funder) relationship
has no temporal dimension at the gold layer.

These tests pin down the contract that the delimited-bridge transformer
must collapse the SCD2 fan-out so each (candidate_key, fk_key) pair
appears at most once in the output.
"""

from unittest.mock import MagicMock

from igh_data_transform.transformations.silver_to_gold.core.bridges import (
    transform_bridge,
)
from igh_data_transform.transformations.silver_to_gold.core.transformer import (
    Transformer,
)


def _extractor_with_tables(tables: dict[str, list[dict]]) -> MagicMock:
    def extract_table(table_name, columns=None):
        rows = tables.get(table_name, [])
        if columns:
            yield from ({c: r.get(c) for c in columns} for r in rows)
        else:
            yield from rows

    ext = MagicMock()
    ext.extract_table.side_effect = extract_table
    return ext


def _build_transformer(candidate_rows: list[dict]) -> Transformer:
    ext = _extractor_with_tables({"vin_candidates": candidate_rows})
    transformer = Transformer(ext)
    # Pre-populate the dimension caches the bridge transformer reads via
    # lookup_dimension_key. Surrogate-key values are arbitrary.
    transformer._dim_caches["dim_candidate_core"] = {"cand-1": 8640, "cand-2": 8641}
    transformer._dim_caches["dim_developer"] = {
        "Academy of Military Science": 100,
        "Suzhou Abogen Biosciences Co. Ltd": 101,
        "Walvax Biotechnology Co., Ltd": 102,
    }
    transformer._dim_caches["dim_funder"] = {
        "Gates Foundation": 200,
        "Wellcome Trust": 201,
    }
    return transformer


class TestDelimitedBridgeDeduplication:
    def test_scd2_fanout_collapses_to_one_pair_per_candidate_developer(self):
        # Four SCD2 rows for the same candidate with identical developer
        # text — the bug from the 2026-05-22 QA report (each developer
        # rendered four times in the slide-in).
        four_versions = [
            {
                "candidateid": "cand-1",
                "developersaggregated": (
                    "Academy of Military Science; "
                    "Suzhou Abogen Biosciences Co. Ltd; "
                    "Walvax Biotechnology Co., Ltd"
                ),
            }
        ] * 4

        transformer = _build_transformer(four_versions)
        rows = transform_bridge(transformer, "bridge_candidate_developer")

        pairs = [(r["candidate_key"], r["developer_key"]) for r in rows]
        assert sorted(pairs) == [(8640, 100), (8640, 101), (8640, 102)]

    def test_distinct_candidates_keep_their_pairs(self):
        # Deduplication must be per-candidate, not global — two
        # candidates pointing at the same developer must both appear.
        transformer = _build_transformer(
            [
                {
                    "candidateid": "cand-1",
                    "developersaggregated": "Academy of Military Science",
                },
                {
                    "candidateid": "cand-1",
                    "developersaggregated": "Academy of Military Science",
                },
                {
                    "candidateid": "cand-2",
                    "developersaggregated": "Academy of Military Science",
                },
            ]
        )
        rows = transform_bridge(transformer, "bridge_candidate_developer")

        pairs = sorted((r["candidate_key"], r["developer_key"]) for r in rows)
        assert pairs == [(8640, 100), (8641, 100)]

    def test_funder_bridge_uses_same_dedup_contract(self):
        # bridge_candidate_funder shares the transform_delimited_bridge
        # code path — same SCD2 fan-out, same expected collapse.
        transformer = _build_transformer(
            [
                {
                    "candidateid": "cand-1",
                    "knownfundersaggregated": "Gates Foundation; Wellcome Trust",
                }
            ]
            * 3
        )
        rows = transform_bridge(transformer, "bridge_candidate_funder")

        pairs = sorted((r["candidate_key"], r["funder_key"]) for r in rows)
        assert pairs == [(8640, 200), (8640, 201)]

    def test_repeated_name_within_a_single_delimited_value_collapses(self):
        # Independent of SCD2, the source text itself can contain the
        # same developer twice (data-entry artefact). The bridge should
        # still emit one pair.
        transformer = _build_transformer(
            [
                {
                    "candidateid": "cand-1",
                    "developersaggregated": (
                        "Academy of Military Science; Academy of Military Science"
                    ),
                }
            ]
        )
        rows = transform_bridge(transformer, "bridge_candidate_developer")

        pairs = [(r["candidate_key"], r["developer_key"]) for r in rows]
        assert pairs == [(8640, 100)]
