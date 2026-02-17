"""Tests for clinical trials transformation."""

import numpy as np
import pandas as pd
import pytest

from igh_data_transform.transformations.clinical_trials import (
    _clean_study_types,
    _synthesize_age_groups,
    _synthesize_gender,
    _synthesize_phase,
    transform_clinical_trials,
)


class TestSynthesizePhase:
    """Tests for _synthesize_phase function."""

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase I", "Phase I"),
            ("PHASE 1", "Phase I"),
            ("1", "Phase I"),
            ("I", "Phase I"),
            ("EARLY_PHASE1", "Phase I"),
            ("PHASE1", "Phase I"),
        ],
    )
    def test_phase_i(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase II", "Phase II"),
            ("PHASE 2", "Phase II"),
            ("2", "Phase II"),
            ("II", "Phase II"),
            ("PHASR II", "Phase II"),
            ("PHASE2", "Phase II"),
        ],
    )
    def test_phase_ii(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase III", "Phase III"),
            ("PHASE 3", "Phase III"),
            ("3", "Phase III"),
            ("III", "Phase III"),
            ("PHASE3", "Phase III"),
        ],
    )
    def test_phase_iii(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase IV", "Phase IV"),
            ("PHASE 4", "Phase IV"),
            ("4", "Phase IV"),
            ("POST-MARKET", "Phase IV"),
            ("POST MARKETING", "Phase IV"),
        ],
    )
    def test_phase_iv(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase I/II", "Phase I/II"),
            ("1/2", "Phase I/II"),
            ("1 AND 2", "Phase I/II"),
            ("PHASE1|PHASE2", "Phase I/II"),
        ],
    )
    def test_combined_i_ii(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase II/III", "Phase II/III"),
            ("2/3", "Phase II/III"),
            ("PHASE2|PHASE3", "Phase II/III"),
        ],
    )
    def test_combined_ii_iii(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Phase III/IV", "Phase III/IV"),
            ("3/4", "Phase III/IV"),
        ],
    )
    def test_combined_iii_iv(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Observational", "Observational"),
            ("CHIM", "CHIM"),
            ("Retrospective", "Retrospective"),
        ],
    )
    def test_special_types_preserved(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("N/A", "N/A"),
            ("NOT APPLICABLE", "N/A"),
            ("Not Applicable", "N/A"),
            ("NA", "N/A"),
            ("0", "N/A"),
            ("PHASE 0", "N/A"),
        ],
    )
    def test_na_values(self, input_val, expected):
        assert _synthesize_phase(input_val) == expected

    def test_none_returns_unknown(self):
        assert _synthesize_phase(None) == "Unknown"

    def test_nan_returns_unknown(self):
        assert _synthesize_phase(float("nan")) == "Unknown"

    def test_unrecognized_returns_unknown(self):
        assert _synthesize_phase("something weird") == "Unknown"


class TestSynthesizeAgeGroups:
    """Tests for _synthesize_age_groups function."""

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Older Adult", "Older adults: 45 >"),
            ("65 Years", "Older adults: 45 >"),
            ("60 Years and older", "Older adults: 45 >"),
            ("50 years", "Older adults: 45 >"),
        ],
    )
    def test_older_adults(self, input_val, expected):
        assert _synthesize_age_groups(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Adult", "Young Adults 18 - 45"),
            ("20 years", "Young Adults 18 - 45"),
            ("25", "Young Adults 18 - 45"),
        ],
    )
    def test_young_adults(self, input_val, expected):
        assert _synthesize_age_groups(input_val) == expected

    def test_18_years_and_older_matches_older_adults(self):
        # "YEARS AND OLDER" pattern matches older adults before "18" matches young adults
        assert _synthesize_age_groups("18 Years and older") == "Older adults: 45 >"

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Adolescent", "Adolescents"),
            ("14 years", "Adolescents"),
            ("17 years", "Adolescents"),
        ],
    )
    def test_adolescents(self, input_val, expected):
        assert _synthesize_age_groups(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Child", "Children"),
            ("Children", "Children"),
        ],
    )
    def test_children(self, input_val, expected):
        assert _synthesize_age_groups(input_val) == expected

    def test_infants(self):
        assert _synthesize_age_groups("Infant") == "Infants"

    def test_18_months_matches_young_adults(self):
        # "18" in "18 MONTHS" matches young adults before infants check
        assert _synthesize_age_groups("18 Months") == "Young Adults 18 - 45"

    def test_neonates(self):
        assert _synthesize_age_groups("Neonate") == "Neonates"

    def test_none_returns_unknown(self):
        assert _synthesize_age_groups(None) == "Unknown"

    def test_nan_returns_unknown(self):
        assert _synthesize_age_groups(float("nan")) == "Unknown"

    def test_unrecognized_returns_unknown(self):
        assert _synthesize_age_groups("something weird") == "Unknown"


class TestSynthesizeGender:
    """Tests for _synthesize_gender function."""

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("Both", "Both"),
            ("All", "Both"),
            ("AND FEMALE", "Both"),
            ("Male and Female", "Both"),
        ],
    )
    def test_both(self, input_val, expected):
        assert _synthesize_gender(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("F", "Female"),
            ("Female", "Female"),
            ("FEMALES", "Female"),
        ],
    )
    def test_female(self, input_val, expected):
        assert _synthesize_gender(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("M", "Male"),
            ("Male", "Male"),
            ("MALES", "Male"),
        ],
    )
    def test_male(self, input_val, expected):
        assert _synthesize_gender(input_val) == expected

    def test_compound_both(self):
        assert _synthesize_gender("Female: yes Male: yes") == "Both"

    def test_compound_female_only(self):
        assert _synthesize_gender("Female: yes Male: no") == "Female"

    def test_compound_male_only(self):
        assert _synthesize_gender("Male: yes Female: no") == "Male"

    def test_none_returns_unknown(self):
        assert _synthesize_gender(None) == "Unknown"

    def test_nan_returns_unknown(self):
        assert _synthesize_gender(float("nan")) == "Unknown"


class TestCleanStudyTypes:
    """Tests for _clean_study_types function."""

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("INTERVENTIONAL", "Interventional"),
            ("Interventional Study", "Interventional"),
            ("Intervention", "Interventional"),
            ("Interventional clinical trial of medicinal product", "Interventional"),
        ],
    )
    def test_interventional(self, input_val, expected):
        assert _clean_study_types(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("OBSERVATIONAL", "Observational"),
            ("Observational Study", "Observational"),
            ("Observational non invasive", "Observational"),
        ],
    )
    def test_observational(self, input_val, expected):
        assert _clean_study_types(input_val) == expected

    def test_passthrough_for_others(self):
        assert _clean_study_types("Basic science") == "Basic science"
        assert _clean_study_types("PMS") == "PMS"

    def test_none_returns_none(self):
        assert _clean_study_types(None) is None

    def test_nan_returns_nan(self):
        result = _clean_study_types(float("nan"))
        assert result is None or (isinstance(result, float) and np.isnan(result))


class TestTransformClinicalTrials:
    """Tests for transform_clinical_trials orchestrator."""

    def _make_input_df(self, overrides=None):
        """Create a minimal input DataFrame mimicking vin_clinicaltrials."""
        data = {
            "row_id": [1, 2],
            "new_studydesign": ["Design A", "Design B"],
            "new_collaborator": [None, "Collab"],
            "new_studydocuments": [None, None],
            "new_test": [None, None],
            "vin_ctphase": ["Phase I", "2"],
            "vin_ctid": [4506, 4498],
            "new_sponsor": ["Sponsor A", "Sponsor B"],
            "new_fundertype": [None, None],
            "new_aim1ctlastupdated": [None, None],
            "modifiedon": ["2025-01-01", "2025-01-02"],
            "importsequencenumber": [None, None],
            "vin_ctterminatedreason": [None, None],
            "vin_clinicaltrialid": ["ct-1", "ct-2"],
            "new_locations": ["USA", "UK"],
            "statecode": [0, 0],
            "_vin_candidate_value": ["cand-1", "cand-2"],
            "new_firstposted": ["2024-01-01", "2024-02-01"],
            "createdon": ["2024-01-01", "2024-02-01"],
            "new_aim1ctnumber": [None, None],
            "new_outcomemeasure_secondary": [None, None],
            "vin_ctresultstype": [None, None],
            "vin_title": ["Title A", "Title B"],
            "vin_enddate": ["2025-12-31", None],
            "new_includedaim1": [None, None],
            "_ownerid_value": ["owner-1", "owner-2"],
            "new_aim1listsctid": [None, None],
            "_modifiedby_value": ["mod-1", "mod-2"],
            "new_outcomemeasure_primary": ["Primary", None],
            "new_aim1ctstatus": [None, None],
            "new_interventions": ["Drug", None],
            "vin_ctresultsstatus": [None, None],
            "vin_ctenrolment": [100, 200],
            "vin_endtype": [None, None],
            "new_aim1pcrreviewnotes": [None, None],
            "new_age": ["Adult", "65 Years"],
            "vin_startdate": ["2024-01-01", "2024-02-01"],
            "new_sex": ["Both", "Female"],
            "new_pipsct": [None, None],
            "_createdby_value": ["cr-1", "cr-2"],
            "vin_ctrialid": ["trial-1", "trial-2"],
            "new_conditions": ["Malaria", "TB"],
            "vin_starttype": [None, None],
            "vin_pcrreviewcomments": [None, None],
            "_owningbusinessunit_value": ["bu-1", "bu-1"],
            "vin_description": ["Desc A", "Desc B"],
            "new_indicationtype": [100000000, 100000001],
            "vin_ctterminatedtype": [None, None],
            "vin_ctresultssource": [None, None],
            "vin_source": ["ClinicalTrials.gov", "WHO ICTRP"],
            "new_resultsfirstposted": [None, None],
            "versionnumber": [100, 200],
            "new_primarycompletiondate": [None, None],
            "new_primaryoutcomemeasures": [None, None],
            "timezoneruleversionnumber": [0.0, None],
            "vin_recentupdates": [None, None],
            "vin_name": ["CT-001", "CT-002"],
            "new_studytype": ["INTERVENTIONAL", "Observational Study"],
            "vin_lastupdated": [None, None],
            "new_secondaryoutcomemeasures": [None, None],
            "_owninguser_value": ["user-1", "user-2"],
            "vin_ctstatus": [100000002.0, 909670002.0],
            "statuscode": [1, 1],
            "json_response": ['{"k":"v"}', '{"k":"v2"}'],
            "sync_time": ["2026-01-09", "2026-01-09"],
            "valid_from": ["2024-01-01", "2024-02-01"],
            "valid_to": [None, None],
            # Empty columns
            "_createdonbehalfby_value": [None, None],
            "overriddencreatedon": [None, None],
            "_modifiedonbehalfby_value": [None, None],
            "utcconversiontimezonecode": [None, None],
            "_owningteam_value": [None, None],
        }
        if overrides:
            data.update(overrides)
        return pd.DataFrame(data)

    def _make_option_sets(self):
        """Create option set dict with vin_ctstatus."""
        return {
            "_optionset_vin_ctstatus": pd.DataFrame({
                "code": [
                    100000001, 100000002, 100000003, 100000004,
                    100000005, 100000006,
                    909670000, 909670001, 909670002, 909670003,
                    909670004, 909670006, 909670007,
                ],
                "label": [
                    "Planned", "Recruiting", "Not yet recruiting",
                    "Active, not recruiting", "Enrolling by invitation",
                    "Not Recruiting",
                    "Terminated", "Active", "Completed",
                    "Results submitted", "Suspended", "Withdrawn", "Unknown",
                ],
                "first_seen": ["2026-01-09"] * 13,
            }),
        }

    def test_drops_metadata_and_aim1_columns(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        dropped = [
            "new_aim1ctlastupdated", "new_aim1ctnumber", "new_includedaim1",
            "_ownerid_value", "new_aim1listsctid", "_modifiedby_value",
            "new_aim1ctstatus", "new_aim1pcrreviewnotes", "new_pipsct",
            "_createdby_value", "vin_pcrreviewcomments",
            "_owningbusinessunit_value", "new_resultsfirstposted",
            "versionnumber", "new_primarycompletiondate",
            "new_primaryoutcomemeasures", "timezoneruleversionnumber",
            "vin_lastupdated", "new_secondaryoutcomemeasures",
            "_owninguser_value", "json_response", "sync_time",
            "new_studydocuments", "vin_ctresultssource",
        ]
        for col in dropped:
            assert col not in result.columns

    def test_drops_empty_columns_preserving_valid_to(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert "valid_to" in result.columns
        assert "_createdonbehalfby_value" not in result.columns

    def test_applies_phase_synthesis(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert list(result["ctphase"]) == ["Phase I", "Phase II"]

    def test_applies_age_group_synthesis(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert list(result["age"]) == ["Young Adults 18 - 45", "Older adults: 45 >"]

    def test_applies_gender_synthesis(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert list(result["sex"]) == ["Both", "Female"]

    def test_applies_study_type_normalization(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert list(result["studytype"]) == ["Interventional", "Observational"]

    def test_ct_status_value_consolidation(self):
        df = self._make_input_df({"vin_ctstatus": [100000002.0, 909670002.0]})
        result, _ = transform_clinical_trials(df)
        # 100000002 (Recruiting) -> 909670001 (Active)
        assert result["ctstatus"].iloc[0] == 909670001
        # 909670002 (Completed) -> unchanged
        assert result["ctstatus"].iloc[1] == 909670002.0

    def test_ct_status_option_set_dedup(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        _, cleaned = transform_clinical_trials(df, option_sets=option_sets)
        assert "_optionset_vin_ctstatus" in cleaned
        os_df = cleaned["_optionset_vin_ctstatus"]
        # Original 13 rows -> 6 rows (remove Planned, Recruiting, Not yet recruiting,
        # Active not recruiting, Enrolling by invitation, Not Recruiting, Results submitted)
        assert len(os_df) == 6
        remaining_labels = set(os_df["label"])
        assert remaining_labels == {
            "Terminated", "Active", "Completed",
            "Suspended", "Withdrawn", "Unknown",
        }

    def test_renames_columns(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert "ctphase" in result.columns
        assert "vin_ctphase" not in result.columns
        assert "age" in result.columns
        assert "new_age" not in result.columns
        assert "sex" in result.columns
        assert "new_sex" not in result.columns
        assert "name" in result.columns
        assert "vin_name" not in result.columns
        assert "studytype" in result.columns
        assert "ctstatus" in result.columns

    def test_returns_tuple(self):
        df = self._make_input_df()
        result, cleaned = transform_clinical_trials(df)
        assert isinstance(result, pd.DataFrame)
        assert isinstance(cleaned, dict)

    def test_does_not_modify_original(self):
        df = self._make_input_df()
        original_columns = list(df.columns)
        transform_clinical_trials(df)
        assert list(df.columns) == original_columns

    def test_preserves_row_count(self):
        df = self._make_input_df()
        result, _ = transform_clinical_trials(df)
        assert len(result) == 2

    def test_works_when_option_sets_is_none(self):
        df = self._make_input_df()
        result, cleaned = transform_clinical_trials(df, option_sets=None)
        assert isinstance(result, pd.DataFrame)
        assert len(cleaned) == 0
