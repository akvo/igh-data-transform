"""Candidates table transformation (vin_candidates)."""

import pandas as pd

from igh_data_transform.transformations._candidates_config import (
    COLUMN_RENAMES,
    COLUMNS_TO_DROP,
)
from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
    replace_values,
)

# Temporal source columns consumed by expansion
_TEMPORAL_SOURCE_COLS = [
    "new_rdstage2021",
    "new_2023currentrdstage",
    "new_2024currentrdstage",
    "_vin_currentrndstage_value",
]

_PRODUCT_TYPE_MAPPING = {
    "Dietary supplement": "Dietary supplements",
    "Diagnostic": "Diagnostics",
    "Drug": "Drugs",
    "Functional foods": "Dietary supplements",
    "Microbial interventions": "Microbial interventions",
    "Chemical vector control products": "VCP",
    "Biological vector control products": "VCP",
    "Vector control products Reservoir targeted vaccines": "VCP",
    "Vector control products": "VCP",
    "Reservoir targeted vaccines": "Vaccines",
}

_RD_STAGE_MAPPING = {
    # → Discovery & Preclinical
    "Discovery": "Discovery & Preclinical",
    "Discovery and Preclinical": "Discovery & Preclinical",
    "Discovery and preclinical": "Discovery & Preclinical",
    "Preclinical": "Discovery & Preclinical",
    "Preclinical - Vaccines": "Discovery & Preclinical",
    "Preclinical - Drugs": "Discovery & Preclinical",
    "Primary and secondary screening and optimisation": "Discovery & Preclinical",
    # → Early development
    "Development": "Early development",
    "Early development (concept and research)": "Early development",
    "Early development (feasibility and planning)": "Early development",
    # → Phase I
    "Phase I - Vaccines": "Phase I",
    # → Phase II  (combo trials map to later phase)
    "Phase II - Vaccines": "Phase II",
    "Phase I/II": "Phase II",
    # → Phase III  (combo trials map to later phase)
    "Phase III - Drugs": "Phase III",
    "Phase III - Vaccines": "Phase III",
    "Phase II/III": "Phase III",
    # → Late development
    "Late development (design and development)": "Late development",
    "Late development (clinical validation and launch readiness)": "Late development",
    "Late development (clinical validation and launch readiness) - Diagnostics": "Late development",
    "Late development - Diagnostics": "Late development",
    "Clinical evaluation": "Late development",
    # → Regulatory filing
    "Regulatory filing - Diagnostics": "Regulatory filing",
    "PQ listing and regulatory approval": "Regulatory filing",
    # → Approved
    "Approved product": "Approved",
    # → Post-marketing surveillance
    "Phase IV": "Post-marketing surveillance",
    "Phase IV - Vaccines": "Post-marketing surveillance",
    "Operational research for diagnostics": "Post-marketing surveillance",
    # → Human safety & efficacy
    "Post-marketing human safety/efficacy studies (without prior clinical studies)": "Human safety & efficacy",
    # Normalize spelling
    "Not applicable": "Not applicable",
    "N/A": "Not applicable",
}

_PRESSURE_TYPE_MAPPING = {
    "Negative pressure ": "Negative pressure",
    "Positive pressure ": "Positive pressure",
    "Not applicable ": "N/A",
}

_APPROVAL_STATUS_CONSOLIDATION = {
    862890001: 909670000,  # Adopted -> Approved
}

_APPROVING_AUTHORITY_CONSOLIDATION = {
    909670002: 909670001,  # SRA Other -> SRA
}

_INDICATION_TYPE_CONSOLIDATION = {
    100000003: 100000001,  # Duplicate Treatment -> Treatment
    100000004: 100000000,  # Duplicate Prevention -> Prevention
    100000005: 100000002,  # Duplicate Prevention & treatment -> Prevention & treatment
}

_PRECLINICAL_RESULTS_CONSOLIDATION = {
    909670004.0: 909670002.0,  # Unavailable/unknown -> Unknown
}

# Option set rows to remove (by code)
_INDICATION_TYPE_CODES_TO_REMOVE = {100000003, 100000004, 100000005}
_PRECLINICAL_RESULTS_CODES_TO_REMOVE = {909670004}
_APPROVAL_STATUS_CODES_TO_REMOVE = {862890001}
_APPROVING_AUTHORITY_CODES_TO_REMOVE = {909670002}


def _resolve_rdstage_fk(
    df: pd.DataFrame,
    rdstageproducts: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve _vin_currentrndstage_value GUIDs to RD stage text names.

    Joins with vin_rdstageproducts and strips the ' - ProductType' suffix
    from vin_name (e.g. 'Phase III - Drugs' -> 'Phase III').
    """
    lookup = rdstageproducts.set_index("vin_rdstageproductid")["vin_name"]
    # Strip product suffix: 'Phase III - Drugs' -> 'Phase III'
    lookup = lookup.str.rsplit(" - ", n=1).str[0]
    df = df.copy()
    df["_resolved_rdstage_2025"] = df["_vin_currentrndstage_value"].map(lookup)
    return df


def _expand_temporal_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create time-versioned rows from year-specific RD stage columns.

    Produces one row per candidate per year where RD stage data exists.
    valid_to = start of candidate's next populated period (None for latest).
    Must be called before column renaming (uses original bronze names).
    """
    year_configs = [
        ("new_rdstage2021", "2021-01-01"),
        ("new_2023currentrdstage", "2023-01-01"),
        ("new_2024currentrdstage", "2024-01-01"),
        ("_resolved_rdstage_2025", "2025-01-01"),
    ]

    frames = []
    for src_col, valid_from in year_configs:
        if src_col not in df.columns:
            continue
        year_df = df[df[src_col].notna()].copy()
        if year_df.empty:
            continue
        year_df["new_currentrdstage"] = year_df[src_col]
        year_df["valid_from"] = valid_from
        frames.append(year_df)

    if not frames:
        df_result = df.copy()
        df_result["new_currentrdstage"] = None
        df_result["valid_from"] = None
        df_result["valid_to"] = None
        cols_to_drop = _TEMPORAL_SOURCE_COLS + ["_resolved_rdstage_2025"]
        return df_result.drop(
            columns=[c for c in cols_to_drop if c in df_result.columns]
        )

    df_expand = pd.concat(frames, ignore_index=True)
    df_expand = df_expand.sort_values(by=["vin_candidateid", "valid_from"])

    # Per-candidate valid_to = next row's valid_from (None for last)
    df_expand["valid_to"] = df_expand.groupby("vin_candidateid")["valid_from"].shift(-1)

    # Drop consumed columns
    cols_to_drop = _TEMPORAL_SOURCE_COLS + ["_resolved_rdstage_2025"]
    df_expand = df_expand.drop(
        columns=[c for c in cols_to_drop if c in df_expand.columns]
    )
    return df_expand


def transform_candidates(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
    lookup_tables: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_candidates table from Bronze to Silver.

    Args:
        df: Raw candidates DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames keyed by table name.
        lookup_tables: Dict of lookup DataFrames (e.g. vin_rdstageproducts).

    Returns:
        Tuple of (transformed DataFrame, dict of cleaned option sets).
    """
    # 1. Resolve FK for 2025 RD stage
    if lookup_tables and "vin_rdstageproducts" in lookup_tables:
        df = _resolve_rdstage_fk(df, lookup_tables["vin_rdstageproducts"])

    # 2. Temporal expansion (reads original bronze column names)
    df = _expand_temporal_rows(df)

    # 3. Drop columns (temporal sources already consumed by expansion)
    df = drop_columns_by_name(df, COLUMNS_TO_DROP)
    df = drop_empty_columns(df, preserve=["valid_to", "valid_from"])

    # 4. Rename columns
    df = rename_columns(df, COLUMN_RENAMES)

    # 5. Standardize new_currentrdstage (from SCD2 expansion)
    if "new_currentrdstage" in df.columns:
        df = replace_values(df, "new_currentrdstage", _RD_STAGE_MAPPING)
        # Strip remaining " - ProductType" suffixes
        mask = df["new_currentrdstage"].str.contains(" - ", na=False)
        df.loc[mask, "new_currentrdstage"] = (
            df.loc[mask, "new_currentrdstage"].str.split(" - ").str[0]
        )
        # Re-apply mapping for values that were only exposed after stripping
        df = replace_values(df, "new_currentrdstage", _RD_STAGE_MAPPING)

    # 6. Standardize other categorical values
    if "pressuretype" in df.columns:
        df = replace_values(df, "pressuretype", _PRESSURE_TYPE_MAPPING)
    if "product" in df.columns:
        df = replace_values(df, "product", _PRODUCT_TYPE_MAPPING)

    # Consolidate option set code values
    if "approvalstatus" in df.columns:
        df = replace_values(df, "approvalstatus", _APPROVAL_STATUS_CONSOLIDATION)
    if "approvingauthority" in df.columns:
        df = replace_values(
            df, "approvingauthority", _APPROVING_AUTHORITY_CONSOLIDATION
        )

    # 7. Filter to pipeline-included candidates
    if "includeinpipeline" in df.columns:
        df = df[
            (df["includeinpipeline"] == 100000000)
            | (df["includeinpipeline"] == 100000002)
        ].copy()

    # 8. Post-filter consolidations
    if "indicationtype" in df.columns:
        df = replace_values(df, "indicationtype", _INDICATION_TYPE_CONSOLIDATION)
    if "preclinicalresultsstatus" in df.columns:
        df = replace_values(
            df, "preclinicalresultsstatus", _PRECLINICAL_RESULTS_CONSOLIDATION
        )

    # Clean option sets
    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets:
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_new_indicationtype",
            _INDICATION_TYPE_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_vin_preclinicalresultsstatus",
            _PRECLINICAL_RESULTS_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_vin_approvalstatus",
            _APPROVAL_STATUS_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_vin_approvingauthority",
            _APPROVING_AUTHORITY_CODES_TO_REMOVE,
        )

    return df, cleaned_option_sets


def _dedup_option_set(
    option_sets: dict[str, pd.DataFrame],
    cleaned: dict[str, pd.DataFrame],
    table_name: str,
    codes_to_remove: set,
) -> None:
    """Remove duplicate codes from an option set table."""
    if table_name in option_sets:
        os_df = option_sets[table_name].copy()
        os_df = os_df[~os_df["code"].isin(codes_to_remove)]
        cleaned[table_name] = os_df.reset_index(drop=True)
