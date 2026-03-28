"""Tests for candidates transformation."""

import pandas as pd

from igh_data_transform.transformations.candidates import (
    transform_candidates,
)


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
            "vin_2019stagepcr": ["Phase I", None, None],
            "new_2024currentrdstage": [
                "Late development (design and development)",
                "Phase III - Drugs",
                "Discovery",
            ],
            "new_2023currentrdstage": ["Phase I", None, None],
            # FK GUID column for 2025 RD stage (resolved via lookup_tables)
            "_vin_currentrndstage_value": ["guid-1", "guid-2", None],
            # Pipeline columns (temporal)
            "vin_2019pcrpipelineinclusion": ["Yes", None, "Yes"],
            "new_includeinpipeline2021": [100000000.0, 100000002.0, 100000001.0],
            "new_2023includeinevgendatabase": ["Yes", "No", "Pending"],
            "new_2024includeinpipeline": [862890000.0, None, None],
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
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        for col in ["row_id", "json_response", "sync_time",
                     "_createdby_value", "_modifiedby_value",
                     "_owninguser_value", "_owningbusinessunit_value",
                     "statuscode", "importsequencenumber", "createdon"]:
            assert col not in result.columns

    def test_drops_empty_columns_preserving_valid_to(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "valid_to" in result.columns
        assert "valid_from" in result.columns

    def test_standardizes_product_types(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        products = list(result["product"].unique())
        assert "Drugs" in products
        assert "Diagnostics" in products
        assert "Vaccines" in products
        assert "Drug" not in products
        assert "Diagnostic" not in products
        assert "Reservoir targeted vaccines" not in products

    def test_standardizes_rd_stages(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        stages = result["new_currentrdstage"].dropna().unique()
        assert "Late development" in stages
        assert "Phase III" in stages
        assert "Discovery & Preclinical" in stages

    def test_rd_stage_strips_product_suffix(self):
        """Remaining ' - ProductType' suffixes are stripped from new_currentrdstage."""
        df = self._make_input_df(overrides={
            "new_2024currentrdstage": ["Phase I - Biologics", None, None],
            "new_2023currentrdstage": [None, None, None],
            "_vin_currentrndstage_value": [None, None, None],
        })
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

    def test_no_includeinpipeline_filtering(self):
        """All candidates are kept regardless of includeinpipeline value."""
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        # All 3 candidates should be present (no filtering)
        candidate_names = result["candidate_name"].unique()
        assert "Candidate A" in candidate_names
        assert "Candidate B" in candidate_names
        assert "Candidate C" in candidate_names

    def test_derives_include_in_pipeline_boolean(self):
        """Only optionset code 100000000 maps to include_in_pipeline=1."""
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "include_in_pipeline" in result.columns
        # Check current period (valid_to is NaN) for each candidate
        cand_a_cur = result[
            (result["candidate_name"] == "Candidate A") & (result["valid_to"].isna())
        ]
        assert (cand_a_cur["include_in_pipeline"] == 1).all()
        cand_b_cur = result[
            (result["candidate_name"] == "Candidate B") & (result["valid_to"].isna())
        ]
        assert (cand_b_cur["include_in_pipeline"] == 0).all()

    def test_preserves_scd2_temporal_columns(self):
        """Temporal expansion produces valid_from/valid_to columns."""
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "valid_from" in result.columns
        assert "valid_to" in result.columns

    def test_renames_columns(self):
        df = self._make_input_df()
        lookup = self._make_lookup_tables()
        result, _ = transform_candidates(df, lookup_tables=lookup)
        assert "candidate_name" in result.columns
        assert "vin_name" not in result.columns
        assert "product" in result.columns
        assert "vin_product" not in result.columns
        # includeinpipeline is now produced by expansion, not rename
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
        result, cleaned = transform_candidates(df, option_sets=None, lookup_tables=lookup)
        assert isinstance(result, pd.DataFrame)
        assert len(cleaned) == 0
