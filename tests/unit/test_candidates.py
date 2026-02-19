"""Tests for candidates transformation."""

import pandas as pd

from igh_data_transform.transformations.candidates import (
    _expand_temporal_rows,
    _resolve_rdstage_fk,
    transform_candidates,
)


class TestResolveRdstageFk:
    """Tests for _resolve_rdstage_fk function."""

    def test_resolves_guid_to_stage_name(self):
        """GUID maps to vin_name with product suffix stripped."""
        df = pd.DataFrame(
            {
                "_vin_currentrndstage_value": ["guid-1", "guid-2"],
                "vin_candidateid": ["c1", "c2"],
            }
        )
        rdstageproducts = pd.DataFrame(
            {
                "vin_rdstageproductid": ["guid-1", "guid-2"],
                "vin_name": ["Phase III - Drugs", "Phase I - Vaccines"],
            }
        )
        result = _resolve_rdstage_fk(df, rdstageproducts)
        assert result["_resolved_rdstage_2025"].iloc[0] == "Phase III"
        assert result["_resolved_rdstage_2025"].iloc[1] == "Phase I"

    def test_unknown_guid_resolves_to_nan(self):
        """Unknown GUID produces NaN in resolved column."""
        df = pd.DataFrame(
            {
                "_vin_currentrndstage_value": ["unknown-guid"],
                "vin_candidateid": ["c1"],
            }
        )
        rdstageproducts = pd.DataFrame(
            {
                "vin_rdstageproductid": ["guid-1"],
                "vin_name": ["Phase III - Drugs"],
            }
        )
        result = _resolve_rdstage_fk(df, rdstageproducts)
        assert pd.isna(result["_resolved_rdstage_2025"].iloc[0])

    def test_compound_name_preserves_prefix(self):
        """'Deactivated - Phase IV - Drugs' -> 'Deactivated - Phase IV'."""
        df = pd.DataFrame(
            {
                "_vin_currentrndstage_value": ["guid-1"],
                "vin_candidateid": ["c1"],
            }
        )
        rdstageproducts = pd.DataFrame(
            {
                "vin_rdstageproductid": ["guid-1"],
                "vin_name": ["Deactivated - Phase IV - Drugs"],
            }
        )
        result = _resolve_rdstage_fk(df, rdstageproducts)
        assert result["_resolved_rdstage_2025"].iloc[0] == "Deactivated - Phase IV"

    def test_does_not_modify_original(self):
        """Original DataFrame is not modified."""
        df = pd.DataFrame(
            {
                "_vin_currentrndstage_value": ["guid-1"],
                "vin_candidateid": ["c1"],
            }
        )
        rdstageproducts = pd.DataFrame(
            {
                "vin_rdstageproductid": ["guid-1"],
                "vin_name": ["Phase III - Drugs"],
            }
        )
        original_cols = list(df.columns)
        _resolve_rdstage_fk(df, rdstageproducts)
        assert list(df.columns) == original_cols


class TestExpandTemporalRows:
    """Tests for _expand_temporal_rows function.

    These tests operate on DataFrames after FK resolution, so 2025 data
    appears in the _resolved_rdstage_2025 column (not vin_currentrdstage).
    """

    def test_candidate_with_all_temporal_years(self):
        """Candidate with data in 2023, 2024, and 2025 produces 3 rows."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": ["Phase I"],
                "new_2024currentrdstage": ["Phase II"],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        assert len(result) == 3
        assert "new_currentrdstage" in result.columns
        stages = list(result["new_currentrdstage"])
        assert "Phase I" in stages
        assert "Phase II" in stages
        assert "Phase III" in stages

    def test_candidate_with_only_some_years(self):
        """Only years with non-null data produce rows."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": [None],
                "new_2024currentrdstage": ["Phase II"],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        assert len(result) == 2

    def test_candidate_with_only_current_year(self):
        """Candidate with only 2025 data produces 1 row."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": [None],
                "new_2024currentrdstage": [None],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        assert len(result) == 1

    def test_rd_stage_column_sourced_correctly(self):
        """Each version carries the correct stage from its source year."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": ["Discovery"],
                "new_2024currentrdstage": ["Preclinical"],
                "_resolved_rdstage_2025": ["Phase I"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        r2023 = result[result["valid_from"].str.startswith("2023")]
        r2024 = result[result["valid_from"].str.startswith("2024")]
        r2025 = result[result["valid_from"].str.startswith("2025")]
        assert r2023["new_currentrdstage"].iloc[0] == "Discovery"
        assert r2024["new_currentrdstage"].iloc[0] == "Preclinical"
        assert r2025["new_currentrdstage"].iloc[0] == "Phase I"

    def test_per_candidate_valid_to_consecutive_years(self):
        """Candidate with 2023, 2024, 2025: valid_to chains to next period."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": ["Phase I"],
                "new_2024currentrdstage": ["Phase II"],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        result = result.sort_values("valid_from").reset_index(drop=True)
        assert result.loc[0, "valid_from"] == "2023-01-01"
        assert result.loc[0, "valid_to"] == "2024-01-01"
        assert result.loc[1, "valid_from"] == "2024-01-01"
        assert result.loc[1, "valid_to"] == "2025-01-01"
        assert result.loc[2, "valid_from"] == "2025-01-01"
        assert pd.isna(result.loc[2, "valid_to"])

    def test_per_candidate_valid_to_with_gap(self):
        """Candidate with 2021 and 2024 (gap): valid_to jumps to next populated period."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_rdstage2021": ["Discovery"],
                "new_2023currentrdstage": [None],
                "new_2024currentrdstage": ["Phase I"],
                "_resolved_rdstage_2025": [None],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        result = result.sort_values("valid_from").reset_index(drop=True)
        assert len(result) == 2
        assert result.loc[0, "valid_from"] == "2021-01-01"
        assert result.loc[0, "valid_to"] == "2024-01-01"
        assert result.loc[1, "valid_from"] == "2024-01-01"
        assert pd.isna(result.loc[1, "valid_to"])

    def test_per_candidate_valid_to_single_year(self):
        """Candidate with only 2025: valid_to = None."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": [None],
                "new_2024currentrdstage": [None],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        assert len(result) == 1
        assert pd.isna(result["valid_to"].iloc[0])

    def test_temporal_source_columns_dropped(self):
        """Year-specific source columns are consumed and removed from output."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": ["Phase I"],
                "new_2024currentrdstage": ["Phase II"],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        for col in [
            "new_2023currentrdstage",
            "new_2024currentrdstage",
            "_resolved_rdstage_2025",
            "new_rdstage2021",
        ]:
            assert col not in result.columns

    def test_null_year_produces_no_row(self):
        """A year with null RD stage produces no row for that year."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1"],
                "vin_name": ["CandA"],
                "new_2023currentrdstage": ["Phase I"],
                "new_2024currentrdstage": [None],
                "_resolved_rdstage_2025": ["Phase III"],
                "vin_product": ["Drugs"],
            }
        )
        result = _expand_temporal_rows(df)
        # 2023 and 2025 have data -> 2 rows; 2024 is null -> no row
        assert len(result) == 2

    def test_multiple_candidates(self):
        """Multiple candidates each get their own SCD2 expansion."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1", "cand-2"],
                "vin_name": ["CandA", "CandB"],
                "new_2023currentrdstage": ["Phase I", "Discovery"],
                "new_2024currentrdstage": ["Phase II", None],
                "_resolved_rdstage_2025": [None, "Preclinical"],
                "vin_product": ["Drugs", "Vaccines"],
            }
        )
        result = _expand_temporal_rows(df)
        # CandA: 2023 + 2024 = 2 rows, CandB: 2023 + 2025 = 2 rows
        assert len(result) == 4

    def test_multiple_candidates_independent_valid_to(self):
        """Each candidate's valid_to is computed from its own periods."""
        df = pd.DataFrame(
            {
                "vin_candidateid": ["cand-1", "cand-2"],
                "vin_name": ["CandA", "CandB"],
                "new_2023currentrdstage": ["Phase I", "Discovery"],
                "new_2024currentrdstage": ["Phase II", None],
                "_resolved_rdstage_2025": [None, "Preclinical"],
                "vin_product": ["Drugs", "Vaccines"],
            }
        )
        result = _expand_temporal_rows(df)
        # CandA: 2023 (valid_to=2024), 2024 (valid_to=None)
        cand_a = result[result["vin_candidateid"] == "cand-1"].sort_values("valid_from")
        assert cand_a.iloc[0]["valid_to"] == "2024-01-01"
        assert pd.isna(cand_a.iloc[1]["valid_to"])
        # CandB: 2023 (valid_to=2025), 2025 (valid_to=None)
        cand_b = result[result["vin_candidateid"] == "cand-2"].sort_values("valid_from")
        assert cand_b.iloc[0]["valid_to"] == "2025-01-01"
        assert pd.isna(cand_b.iloc[1]["valid_to"])


class TestTransformCandidates:
    """Tests for transform_candidates function."""

    def _make_input_df(self, overrides=None):
        """Create a minimal input DataFrame mimicking vin_candidates (bronze columns)."""
        data = {
            # Columns that stay (original bronze names before rename)
            "vin_name": ["Candidate A", "Candidate B", "Candidate C"],
            "vin_product": ["Drug", "Diagnostic", "Reservoir targeted vaccines"],
            "new_pressuretype": ["Negative pressure ", "Not applicable ", None],
            # Temporal source columns (consumed by SCD2 expansion)
            "new_2024currentrdstage": [
                "Late development (design and development)",
                "Phase III - Drugs",
                "Discovery",
            ],
            "new_2023currentrdstage": ["Phase I", None, None],
            # FK GUID column for 2025 RD stage (resolved via lookup_tables)
            "_vin_currentrndstage_value": ["guid-1", "guid-2", None],
            "new_includeinpipeline": [100000000.0, 100000002.0, 100000001.0],
            "_vin_captype_value": [
                "c1746ad3-93d1-f011-bbd3-00224892cefa",
                "545d63d9-93d1-f011-bbd3-00224892cefa",
                None,
            ],
            "vin_approvalstatus": [862890001.0, 909670000.0, None],
            "vin_approvingauthority": [909670002.0, 909670001.0, None],
            "new_indicationtype": [100000003.0, 100000000.0, None],
            "vin_preclinicalresultsstatus": [909670004.0, 909670000.0, None],
            "vin_candidateid": ["id-1", "id-2", "id-3"],
            "modifiedon": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "statecode": [0, 0, 0],
            # Columns to drop (metadata)
            "row_id": [1, 2, 3],
            "json_response": ['{"k":"v"}', '{"k":"v2"}', '{"k":"v3"}'],
            "sync_time": ["2026-01-09", "2026-01-09", "2026-01-09"],
            "_createdby_value": ["u1", "u2", "u3"],
            "_modifiedby_value": ["m1", "m2", "m3"],
            "_owninguser_value": ["o1", "o2", "o3"],
            "_owningbusinessunit_value": ["b1", "b1", "b1"],
            "statuscode": [1, 1, 1],
            "importsequencenumber": [None, None, None],
            "createdon": ["2024-01-01", "2024-01-02", "2024-01-03"],
            # All-null columns
            "valid_to_empty": [None, None, None],
        }
        if overrides:
            data.update(overrides)
        return pd.DataFrame(data)

    def _make_lookup_tables(self):
        """Create lookup_tables dict with vin_rdstageproducts."""
        return {
            "vin_rdstageproducts": pd.DataFrame(
                {
                    "vin_rdstageproductid": ["guid-1", "guid-2"],
                    "vin_name": ["Phase II - Drugs", "Phase III - Vaccines"],
                }
            ),
        }

    def _make_option_sets(self):
        """Create option sets dict for candidates."""
        return {
            "_optionset_new_indicationtype": pd.DataFrame(
                {
                    "code": [
                        100000000,
                        100000001,
                        100000002,
                        100000003,
                        100000004,
                        100000005,
                    ],
                    "label": [
                        "Prevention",
                        "Treatment",
                        "Prevention & treatment",
                        "Treatment",
                        "Prevention",
                        "Prevention & treatment",
                    ],
                    "first_seen": ["2026-01-09"] * 6,
                }
            ),
            "_optionset_vin_preclinicalresultsstatus": pd.DataFrame(
                {
                    "code": [909670000, 909670001, 909670002, 909670003, 909670004],
                    "label": [
                        "Available",
                        "Unavailable",
                        "Unknown",
                        "N/A",
                        "Unavailable/unknown",
                    ],
                    "first_seen": ["2026-01-09"] * 5,
                }
            ),
            "_optionset_vin_approvalstatus": pd.DataFrame(
                {
                    "code": [
                        862890001,
                        862890002,
                        909670000,
                        909670002,
                        909670003,
                        909670004,
                        909670005,
                    ],
                    "label": [
                        "Adopted",
                        "Used off-label",
                        "Approved",
                        "Approval withdrawn",
                        "Emergency Use Authorisation",
                        "Application under review",
                        "Approval status unclear",
                    ],
                    "first_seen": ["2026-01-09"] * 7,
                }
            ),
            "_optionset_vin_approvingauthority": pd.DataFrame(
                {
                    "code": [909670000, 909670001, 909670002, 909670003],
                    "label": ["NRA", "SRA", "SRA Other", "WHO prequalification"],
                    "first_seen": ["2026-01-09"] * 4,
                }
            ),
        }

    def test_drops_metadata_columns(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        for col in [
            "row_id",
            "json_response",
            "sync_time",
            "_createdby_value",
            "_modifiedby_value",
            "_owninguser_value",
            "_owningbusinessunit_value",
            "statuscode",
            "importsequencenumber",
            "createdon",
        ]:
            assert col not in result.columns

    def test_drops_empty_columns_preserving_valid_to(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "valid_to" in result.columns
        assert "valid_from" in result.columns

    def test_standardizes_product_types(self):
        df = self._make_input_df(
            overrides={
                "new_includeinpipeline": [100000000.0, 100000002.0, 100000000.0],
            }
        )
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        products = list(result["product"].unique())
        assert "Drugs" in products
        assert "Diagnostics" in products
        assert "Vaccines" in products
        assert "Drug" not in products
        assert "Diagnostic" not in products
        assert "Reservoir targeted vaccines" not in products

    def test_standardizes_rd_stages_via_temporal_expansion(self):
        """RD stages are standardized on the new_currentrdstage column."""
        df = self._make_input_df(
            overrides={
                "new_includeinpipeline": [100000000.0, 100000002.0, 100000000.0],
            }
        )
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        stages = result["new_currentrdstage"].dropna().unique()
        assert "Late development" in stages
        assert "Phase III" in stages
        assert "Discovery & Preclinical" in stages

    def test_rd_stage_suffix_stripping(self):
        """Remaining ' - ProductType' suffixes are stripped from new_currentrdstage."""
        df = self._make_input_df(
            overrides={
                "new_2024currentrdstage": ["Phase I - Biologics", None, None],
                "new_2023currentrdstage": [None, None, None],
                "_vin_currentrndstage_value": [None, None, None],
                "new_includeinpipeline": [100000000.0, 100000002.0, 100000001.0],
            }
        )
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        cand_a = result[result["candidate_name"] == "Candidate A"]
        stages = cand_a["new_currentrdstage"].dropna().tolist()
        for stage in stages:
            assert " - " not in stage

    def test_standardizes_pressure_types(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        pressures = list(result["pressuretype"].dropna().unique())
        assert "Negative pressure" in pressures
        assert "N/A" in pressures
        assert "Negative pressure " not in pressures
        assert "Not applicable " not in pressures

    def test_filters_by_includeinpipeline(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        # Candidate C has includeinpipeline=100000001 -> filtered out
        # Candidates A and B have 100000000 and 100000002 -> kept
        candidate_names = result["candidate_name"].unique()
        assert "Candidate A" in candidate_names
        assert "Candidate B" in candidate_names
        assert "Candidate C" not in candidate_names

    def test_temporal_expansion_produces_new_currentrdstage(self):
        """Transform produces new_currentrdstage base column from SCD2 expansion."""
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "new_currentrdstage" in result.columns
        assert len(result) >= 2

    def test_temporal_source_columns_removed_after_transform(self):
        """Year-specific temporal columns are consumed and not in final output."""
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        for col in [
            "new_2023currentrdstage",
            "new_2024currentrdstage",
            "_vin_currentrndstage_value",
            "_resolved_rdstage_2025",
            "new_rdstage2021",
        ]:
            assert col not in result.columns

    def test_captype_value_preserved(self):
        """_vin_captype_value is renamed to captype_value and preserved."""
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "captype_value" in result.columns
        assert "_vin_captype_value" not in result.columns

    def test_renames_columns(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "candidate_name" in result.columns
        assert "vin_name" not in result.columns
        assert "product" in result.columns
        assert "vin_product" not in result.columns
        assert "includeinpipeline" in result.columns
        assert "candidateid" in result.columns

    def test_approval_status_consolidation(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        # 862890001 (Adopted) -> 909670000 (Approved)
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["approvalstatus"] == 909670000).all()

    def test_approving_authority_consolidation(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        # 909670002 (SRA Other) -> 909670001 (SRA)
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["approvingauthority"] == 909670001).all()

    def test_indication_type_consolidation(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        # 100000003 -> 100000001
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["indicationtype"] == 100000001).all()

    def test_preclinical_results_consolidation(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        # 909670004 -> 909670002
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["preclinicalresultsstatus"] == 909670002.0).all()

    def test_option_set_dedup_indication_type(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        lookup = self._make_lookup_tables()
        _, cleaned = transform_candidates(
            df, option_sets=option_sets, lookup_tables=lookup
        )
        assert "_optionset_new_indicationtype" in cleaned
        os_df = cleaned["_optionset_new_indicationtype"]
        assert len(os_df) == 3  # 6 -> 3 (remove duplicates at 100000003-5)

    def test_option_set_dedup_preclinical_results(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        lookup = self._make_lookup_tables()
        _, cleaned = transform_candidates(
            df, option_sets=option_sets, lookup_tables=lookup
        )
        assert "_optionset_vin_preclinicalresultsstatus" in cleaned
        os_df = cleaned["_optionset_vin_preclinicalresultsstatus"]
        assert len(os_df) == 4  # 5 -> 4 (remove Unavailable/unknown)

    def test_option_set_dedup_approval_status(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        lookup = self._make_lookup_tables()
        _, cleaned = transform_candidates(
            df, option_sets=option_sets, lookup_tables=lookup
        )
        assert "_optionset_vin_approvalstatus" in cleaned
        os_df = cleaned["_optionset_vin_approvalstatus"]
        assert len(os_df) == 6  # 7 -> 6 (remove Adopted)

    def test_option_set_dedup_approving_authority(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        lookup = self._make_lookup_tables()
        _, cleaned = transform_candidates(
            df, option_sets=option_sets, lookup_tables=lookup
        )
        assert "_optionset_vin_approvingauthority" in cleaned
        os_df = cleaned["_optionset_vin_approvingauthority"]
        assert len(os_df) == 3  # 4 -> 3 (remove SRA Other)

    def test_returns_tuple(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, cleaned = transform_candidates(df, lookup_tables=lookup)
        assert isinstance(result, pd.DataFrame)
        assert isinstance(cleaned, dict)

    def test_does_not_modify_original(self):
        df = self._make_input_df()
        original_columns = list(df.columns)
        lookup = self._make_lookup_tables()
        transform_candidates(df, lookup_tables=lookup)
        assert list(df.columns) == original_columns

    def test_works_when_option_sets_is_none(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, cleaned = transform_candidates(
            df, option_sets=None, lookup_tables=lookup
        )
        assert isinstance(result, pd.DataFrame)
        assert len(cleaned) == 0

    def test_works_without_lookup_tables(self):
        """Transform still works when lookup_tables is None (no FK resolution)."""
        df = self._make_input_df()
        result, cleaned = transform_candidates(df, lookup_tables=None)
        assert isinstance(result, pd.DataFrame)
        # _vin_currentrndstage_value should be dropped (in _COLUMNS_TO_DROP)
        assert "_vin_currentrndstage_value" not in result.columns
