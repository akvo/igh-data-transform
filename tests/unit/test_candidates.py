"""Tests for candidates transformation."""

import pandas as pd

from igh_data_transform.transformations.candidates import (
    _expand_temporal_rows,
    transform_candidates,
)


class TestExpandTemporalRows:
    """Tests for _expand_temporal_rows function."""

    def test_candidate_with_all_three_years(self):
        df = pd.DataFrame({
            "candidate_name": ["CandA"],
            "2023currentrdstage": ["Phase I"],
            "2024rdstage": ["Phase II"],
            "rdstage_value": ["Phase III"],
            "product": ["Drugs"],
        })
        result = _expand_temporal_rows(df)
        assert len(result) == 3
        assert "RD_stage" in result.columns
        stages = list(result["RD_stage"])
        assert "Phase I" in stages
        assert "Phase II" in stages
        assert "Phase III" in stages

    def test_candidate_with_only_some_years(self):
        df = pd.DataFrame({
            "candidate_name": ["CandA"],
            "2023currentrdstage": [None],
            "2024rdstage": ["Phase II"],
            "rdstage_value": ["Phase III"],
            "product": ["Drugs"],
        })
        result = _expand_temporal_rows(df)
        assert len(result) == 2

    def test_candidate_with_only_one_year(self):
        df = pd.DataFrame({
            "candidate_name": ["CandA"],
            "2023currentrdstage": [None],
            "2024rdstage": [None],
            "rdstage_value": ["Phase III"],
            "product": ["Drugs"],
        })
        result = _expand_temporal_rows(df)
        assert len(result) == 1

    def test_rd_stage_column_sourced_correctly(self):
        df = pd.DataFrame({
            "candidate_name": ["CandA"],
            "2023currentrdstage": ["Discovery"],
            "2024rdstage": ["Preclinical"],
            "rdstage_value": ["Phase I"],
            "product": ["Drugs"],
        })
        result = _expand_temporal_rows(df)
        r2023 = result[result["valid_from"] == "2023-01-01"]
        r2024 = result[result["valid_from"] == "2024-01-01"]
        r2025 = result[result["valid_from"] == "2025-01-01"]
        assert r2023["RD_stage"].iloc[0] == "Discovery"
        assert r2024["RD_stage"].iloc[0] == "Preclinical"
        assert r2025["RD_stage"].iloc[0] == "Phase I"

    def test_valid_from_valid_to_dates_correct(self):
        df = pd.DataFrame({
            "candidate_name": ["CandA"],
            "2023currentrdstage": ["Phase I"],
            "2024rdstage": ["Phase II"],
            "rdstage_value": ["Phase III"],
            "product": ["Drugs"],
        })
        result = _expand_temporal_rows(df)
        r2023 = result[result["valid_from"] == "2023-01-01"]
        assert r2023["valid_to"].iloc[0] == "2023-12-31"
        r2024 = result[result["valid_from"] == "2024-01-01"]
        assert r2024["valid_to"].iloc[0] == "2024-12-31"
        r2025 = result[result["valid_from"] == "2025-01-01"]
        assert r2025["valid_to"].iloc[0] == "2025-12-31"

    def test_result_sorted_by_candidate_name(self):
        df = pd.DataFrame({
            "candidate_name": ["Zebra", "Alpha"],
            "2023currentrdstage": [None, None],
            "2024rdstage": [None, None],
            "rdstage_value": ["Phase I", "Phase II"],
            "product": ["Drugs", "Vaccines"],
        })
        result = _expand_temporal_rows(df)
        assert list(result["candidate_name"]) == ["Alpha", "Zebra"]

    def test_multiple_candidates_with_multiple_years(self):
        df = pd.DataFrame({
            "candidate_name": ["CandA", "CandB"],
            "2023currentrdstage": ["Phase I", "Discovery"],
            "2024rdstage": ["Phase II", None],
            "rdstage_value": [None, "Preclinical"],
            "product": ["Drugs", "Vaccines"],
        })
        result = _expand_temporal_rows(df)
        # CandA: 2023 + 2024 = 2 rows, CandB: 2023 + 2025 = 2 rows
        assert len(result) == 4


class TestTransformCandidates:
    """Tests for transform_candidates function."""

    def _make_input_df(self, overrides=None):
        """Create a minimal input DataFrame mimicking vin_candidates."""
        data = {
            # Columns that stay
            "vin_name": ["Candidate A", "Candidate B", "Candidate C"],
            "vin_product": ["Drug", "Diagnostic", "Reservoir targeted vaccines"],
            "new_pressuretype": ["Negative pressure ", "Not applicable ", None],
            "new_2024currentrdstage": [
                "Late development (design and development)",
                "Phase III - Drugs",
                "Discovery",
            ],
            "new_includeinpipeline": [100000000.0, 100000002.0, 100000001.0],
            "new_2023currentrdstage": ["Phase I", None, None],
            "_vin_currentrndstage_value": ["Phase II", "Phase III", None],
            "vin_approvalstatus": [862890001.0, 909670000.0, None],
            "vin_approvingauthority": [909670002.0, 909670001.0, None],
            "new_indicationtype": [100000003.0, 100000000.0, None],
            "vin_preclinicalresultsstatus": [909670004.0, 909670000.0, None],
            "vin_candidateid": ["id-1", "id-2", "id-3"],
            "modifiedon": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "statecode": [0, 0, 0],
            "valid_from": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "valid_to": [None, None, None],
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

    def _make_option_sets(self):
        """Create option sets dict for candidates."""
        return {
            "_optionset_new_indicationtype": pd.DataFrame({
                "code": [100000000, 100000001, 100000002, 100000003, 100000004, 100000005],
                "label": [
                    "Prevention", "Treatment", "Prevention & treatment",
                    "Treatment", "Prevention", "Prevention & treatment",
                ],
                "first_seen": ["2026-01-09"] * 6,
            }),
            "_optionset_vin_preclinicalresultsstatus": pd.DataFrame({
                "code": [909670000, 909670001, 909670002, 909670003, 909670004],
                "label": [
                    "Available", "Unavailable", "Unknown", "N/A", "Unavailable/unknown",
                ],
                "first_seen": ["2026-01-09"] * 5,
            }),
            "_optionset_vin_approvalstatus": pd.DataFrame({
                "code": [862890001, 862890002, 909670000, 909670002, 909670003, 909670004, 909670005],
                "label": [
                    "Adopted", "Used off-label", "Approved", "Approval withdrawn",
                    "Emergency Use Authorisation", "Application under review",
                    "Approval status unclear",
                ],
                "first_seen": ["2026-01-09"] * 7,
            }),
            "_optionset_vin_approvingauthority": pd.DataFrame({
                "code": [909670000, 909670001, 909670002, 909670003],
                "label": ["NRA", "SRA", "SRA Other", "WHO prequalification"],
                "first_seen": ["2026-01-09"] * 4,
            }),
        }

    def test_drops_metadata_columns(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        for col in ["row_id", "json_response", "sync_time",
                     "_createdby_value", "_modifiedby_value",
                     "_owninguser_value", "_owningbusinessunit_value",
                     "statuscode", "importsequencenumber", "createdon"]:
            assert col not in result.columns

    def test_drops_empty_columns_preserving_valid_to(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        assert "valid_to" in result.columns
        assert "valid_from" in result.columns

    def test_standardizes_product_types(self):
        df = self._make_input_df(overrides={
            "new_includeinpipeline": [100000000.0, 100000002.0, 100000000.0],
        })
        result, _ = transform_candidates(df)
        products = list(result["product"].unique())
        assert "Drugs" in products
        assert "Diagnostics" in products
        assert "Vaccines" in products
        assert "Drug" not in products
        assert "Diagnostic" not in products
        assert "Reservoir targeted vaccines" not in products

    def test_standardizes_rd_stages(self):
        df = self._make_input_df(overrides={
            "new_includeinpipeline": [100000000.0, 100000002.0, 100000000.0],
        })
        result, _ = transform_candidates(df)
        stages = result["2024rdstage"].dropna().unique()
        assert "Late development" in stages
        assert "Phase III" in stages
        assert "Discovery and Preclinical" in stages

    def test_standardizes_pressure_types(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        pressures = list(result["pressuretype"].dropna().unique())
        assert "Negative pressure" in pressures
        assert "N/A" in pressures
        assert "Negative pressure " not in pressures
        assert "Not applicable " not in pressures

    def test_filters_by_includeinpipeline(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        # Candidate C has includeinpipeline=100000001 -> filtered out
        # Candidates A and B have 100000000 and 100000002 -> kept
        candidate_names = result["candidate_name"].unique()
        assert "Candidate A" in candidate_names
        assert "Candidate B" in candidate_names
        assert "Candidate C" not in candidate_names

    def test_calls_temporal_expansion(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        assert "RD_stage" in result.columns
        # Candidate A: 2023 + 2025 = 2 rows, Candidate B: 2025 = 1 row
        assert len(result) >= 2

    def test_renames_columns(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        assert "candidate_name" in result.columns
        assert "vin_name" not in result.columns
        assert "product" in result.columns
        assert "vin_product" not in result.columns
        assert "includeinpipeline" in result.columns
        assert "candidateid" in result.columns

    def test_approval_status_consolidation(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        # 862890001 (Adopted) -> 909670000 (Approved)
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["approvalstatus"] == 909670000).all()

    def test_approving_authority_consolidation(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        # 909670002 (SRA Other) -> 909670001 (SRA)
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["approvingauthority"] == 909670001).all()

    def test_indication_type_consolidation(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        # 100000003 -> 100000001
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["indicationtype"] == 100000001).all()

    def test_preclinical_results_consolidation(self):
        df = self._make_input_df()
        result, _ = transform_candidates(df)
        # 909670004 -> 909670002
        cand_a = result[result["candidate_name"] == "Candidate A"]
        assert (cand_a["preclinicalresultsstatus"] == 909670002.0).all()

    def test_option_set_dedup_indication_type(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        _, cleaned = transform_candidates(df, option_sets=option_sets)
        assert "_optionset_new_indicationtype" in cleaned
        os_df = cleaned["_optionset_new_indicationtype"]
        assert len(os_df) == 3  # 6 -> 3 (remove duplicates at 100000003-5)

    def test_option_set_dedup_preclinical_results(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        _, cleaned = transform_candidates(df, option_sets=option_sets)
        assert "_optionset_vin_preclinicalresultsstatus" in cleaned
        os_df = cleaned["_optionset_vin_preclinicalresultsstatus"]
        assert len(os_df) == 4  # 5 -> 4 (remove Unavailable/unknown)

    def test_option_set_dedup_approval_status(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        _, cleaned = transform_candidates(df, option_sets=option_sets)
        assert "_optionset_vin_approvalstatus" in cleaned
        os_df = cleaned["_optionset_vin_approvalstatus"]
        assert len(os_df) == 6  # 7 -> 6 (remove Adopted)

    def test_option_set_dedup_approving_authority(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        _, cleaned = transform_candidates(df, option_sets=option_sets)
        assert "_optionset_vin_approvingauthority" in cleaned
        os_df = cleaned["_optionset_vin_approvingauthority"]
        assert len(os_df) == 3  # 4 -> 3 (remove SRA Other)

    def test_returns_tuple(self):
        df = self._make_input_df()
        result, cleaned = transform_candidates(df)
        assert isinstance(result, pd.DataFrame)
        assert isinstance(cleaned, dict)

    def test_does_not_modify_original(self):
        df = self._make_input_df()
        original_columns = list(df.columns)
        transform_candidates(df)
        assert list(df.columns) == original_columns

    def test_works_when_option_sets_is_none(self):
        df = self._make_input_df()
        result, cleaned = transform_candidates(df, option_sets=None)
        assert isinstance(result, pd.DataFrame)
        assert len(cleaned) == 0
