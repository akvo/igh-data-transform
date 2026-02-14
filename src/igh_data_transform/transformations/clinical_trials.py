"""Clinical trials table transformation (vin_clinicaltrials)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
    replace_values,
)

_COLUMNS_TO_DROP = [
    "new_aim1ctlastupdated",
    "new_aim1ctnumber",
    "new_includedaim1",
    "_ownerid_value",
    "new_aim1listsctid",
    "_modifiedby_value",
    "new_aim1ctstatus",
    "new_aim1pcrreviewnotes",
    "new_pipsct",
    "_createdby_value",
    "vin_pcrreviewcomments",
    "_owningbusinessunit_value",
    "new_resultsfirstposted",
    "versionnumber",
    "new_primarycompletiondate",
    "new_primaryoutcomemeasures",
    "timezoneruleversionnumber",
    "vin_lastupdated",
    "new_secondaryoutcomemeasures",
    "_owninguser_value",
    "json_response",
    "sync_time",
    "new_studydocuments",
    "vin_ctresultssource",
]

_COLUMN_RENAMES = {
    "new_studydesign": "study_design",
    "new_collaborator": "collaborator",
    "new_test": "test",
    "vin_ctphase": "ctphase",
    "vin_ctid": "ctid",
    "new_sponsor": "sponsor",
    "new_fundertype": "fundertype",
    "vin_ctterminatedreason": "ctterminatedreason",
    "vin_clinicaltrialid": "clinicaltrialid",
    "new_locations": "locations",
    "_vin_candidate_value": "candidate_value",
    "new_firstposted": "firstposted",
    "new_outcomemeasure_secondary": "outcomemeasure_secondary",
    "vin_ctresultstype": "ctresultstype",
    "vin_title": "title",
    "vin_enddate": "enddate",
    "new_outcomemeasure_primary": "outcomemeasure_primary",
    "new_interventions": "interventions",
    "vin_ctresultsstatus": "ctresultsstatus",
    "vin_ctenrolment": "cttenrolment",
    "vin_endtype": "endtype",
    "new_age": "age",
    "vin_startdate": "startdate",
    "new_sex": "sex",
    "vin_ctrialid": "trialid",
    "new_conditions": "conditions",
    "vin_starttype": "starttype",
    "vin_description": "description",
    "new_indicationtype": "indicationtype",
    "vin_ctterminatedtype": "ctterminatedtype",
    "vin_recentupdates": "recentupdates",
    "vin_name": "name",
    "new_studytype": "studytype",
    "vin_ctstatus": "ctstatus",
}

_CT_STATUS_CONSOLIDATION = {
    100000001.0: 909670001,  # Planned -> Active
    100000002.0: 909670001,  # Recruiting -> Active
    100000003.0: 909670001,  # Not yet recruiting -> Active
    100000004.0: 909670001,  # Active, not recruiting -> Active
    100000005.0: 909670001,  # Enrolling by invitation -> Active
    100000006.0: 909670001,  # Not Recruiting -> Active
    909670003.0: 909670001,  # Results submitted -> Active
}

# Codes to remove from the option set (consolidated into Active)
_CT_STATUS_CODES_TO_REMOVE = {
    100000001, 100000002, 100000003, 100000004,
    100000005, 100000006, 909670003,
}


def _synthesize_phase(val) -> str:
    """Standardize clinical trial phase values."""
    if pd.isna(val) or val == "None":
        return "Unknown"

    v = str(val).upper().strip().replace("\n", " ")

    # N/A (exact match)
    na_terms = {
        "N/A", "NOT APPLICABLE", "PHASE N/A", "PHASE: N/A",
        "NA", "0", "PHASE 0",
    }
    if v in na_terms:
        return "N/A"

    # Unknown (exact match)
    if v in {"UNKNOWN", "PHASE UNSPECIFIED", "OTHER", "NOT SELECTED"}:
        return "Unknown"

    # Combined phases - check most specific first to avoid substring collision
    if any(x in v for x in ["III/IV", "3/4"]):
        return "Phase III/IV"
    if any(x in v for x in ["II/III", "2/3", "PHASE2|PHASE3"]):
        return "Phase II/III"
    if any(x in v for x in ["I/II", "1/2", "1 AND 2", "PHASE1|PHASE2"]) or v == "12":
        return "Phase I/II"

    # Specialized study types (before single-phase checks to avoid "I" collisions)
    if "OBSERVATIONAL" in v:
        return "Observational"
    if "INTERVENTIONAL" in v:
        return "Interventional"
    if "RETROSPECTIVE" in v:
        return "Retrospective"
    if "CHIM" in v:
        return "CHIM"

    # Single phases - check highest first; use substring for multi-word,
    # exact match for single-char to avoid "I" in "PHASE IV" collisions
    phase_iv_terms = ["PHASE 4", "PHASE IV", "POST-MARKET", "POST MARKETING"]
    if any(x in v for x in phase_iv_terms) or v in {"IV", "4"}:
        return "Phase IV"

    phase_iii_terms = ["PHASE 3", "PHASE III", "PHASE3"]
    if any(x in v for x in phase_iii_terms) or v in {"III", "3"}:
        return "Phase III"

    phase_ii_terms = ["PHASE 2", "PHASE II", "PHASR II", "PHASE2"]
    if any(x in v for x in phase_ii_terms) or v in {"II", "2"}:
        return "Phase II"

    phase_i_terms = ["PHASE 1", "PHASE I", "EARLY_PHASE1", "PHASE1"]
    if any(x in v for x in phase_i_terms) or v in {"I", "1"}:
        return "Phase I"

    return "Unknown"


def _synthesize_age_groups(val) -> str:
    """Standardize age group values."""
    if pd.isna(val) or val == "None":
        return "Unknown"

    v = str(val).upper().replace("\xa0", " ").replace("Â", " ").strip()

    # Older adults: 45 >
    if any(x in v for x in [
        "OLDER ADULT", "OLDER_ADULT", "(OLDER)",
        "65", "64", "60", "55", "50", "49", "48",
    ]):
        return "Older adults: 45 >"
    if "YEARS AND OLDER" in v or "YEARS AND OVER" in v:
        return "Older adults: 45 >"

    # Young Adults 18 - 45
    if "ADULT" in v or "18" in v or "20" in v or "25" in v or "18-45" in v or "18-40" in v:
        return "Young Adults 18 - 45"

    # Adolescents
    if "ADOLESCENT" in v or any(x in v for x in ["14", "15", "16", "17"]):
        return "Adolescents"

    # Children
    if "CHILD" in v or "CHILDREN" in v:
        return "Children"

    # Infants
    if "INFANT" in v or "18 MONTHS" in v:
        return "Infants"

    # Neonates
    if "NEONATE" in v:
        return "Neonates"

    return "Unknown"


def _synthesize_gender(val) -> str:
    """Standardize gender values."""
    if pd.isna(val) or val == "None":
        return "Unknown"

    v = str(val).upper().replace("<BR>", " ").strip()

    # Simple keyword checks
    if any(x in v for x in ["BOTH", "ALL", "AND FEMALE", "AND MALE"]):
        return "Both"

    # Compound patterns: "FEMALE: YES" contains "MALE: YES" as substring,
    # so check the explicit NO patterns first for specificity
    if "FEMALE: YES" in v and "MALE: NO" in v:
        return "Female"
    if "MALE: YES" in v and "FEMALE: NO" in v:
        return "Male"
    # For compound Both, check MALE: YES appears after FEMALE: YES
    if "FEMALE: YES" in v:
        remaining = v[v.index("FEMALE: YES") + len("FEMALE: YES"):]
        if "MALE: YES" in remaining:
            return "Both"

    # Exact match for simple values
    if v in ("F", "FEMALE", "FEMALES"):
        return "Female"
    if v in ("M", "MALE", "MALES"):
        return "Male"

    return "Unknown"


def _clean_study_types(val):
    """Standardize study type values."""
    if pd.isna(val):
        return val

    v_clean = str(val).strip().upper()

    interventional_variants = [
        "INTERVENTIONAL", "INTERVENTIONAL STUDY",
        "INTERVENTION", "INTERVENTIONAL CLINICAL TRIAL OF MEDICINAL PRODUCT",
    ]
    if v_clean in interventional_variants:
        return "Interventional"

    observational_variants = [
        "OBSERVATIONAL", "OBSERVATIONAL STUDY",
        "OBSERVATIONAL NON INVASIVE",
    ]
    if v_clean in observational_variants:
        return "Observational"

    return val


def transform_clinical_trials(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_clinicaltrials table from Bronze to Silver.

    Args:
        df: Raw clinical trials DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames keyed by table name.

    Returns:
        Tuple of (transformed DataFrame, dict of cleaned option sets).
    """
    df = df.copy()

    # Strip whitespace from age column before synthesis
    if "new_age" in df.columns:
        df["new_age"] = df["new_age"].str.strip()

    # Apply synthesis functions
    if "vin_ctphase" in df.columns:
        df["vin_ctphase"] = df["vin_ctphase"].apply(_synthesize_phase)
    if "new_age" in df.columns:
        df["new_age"] = df["new_age"].apply(_synthesize_age_groups)
    if "new_sex" in df.columns:
        df["new_sex"] = df["new_sex"].apply(_synthesize_gender)
    if "new_studytype" in df.columns:
        df["new_studytype"] = df["new_studytype"].apply(_clean_study_types)

    # Consolidate CT status values
    if "vin_ctstatus" in df.columns:
        df = replace_values(df, "vin_ctstatus", _CT_STATUS_CONSOLIDATION)

    # Drop columns
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)
    df = drop_empty_columns(df, preserve=["valid_to"])

    # Rename columns
    df = rename_columns(df, _COLUMN_RENAMES)

    # Clean option sets
    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets and "_optionset_vin_ctstatus" in option_sets:
        os_df = option_sets["_optionset_vin_ctstatus"].copy()
        os_df = os_df[~os_df["code"].isin(_CT_STATUS_CODES_TO_REMOVE)]
        cleaned_option_sets["_optionset_vin_ctstatus"] = os_df.reset_index(drop=True)

    return df, cleaned_option_sets
