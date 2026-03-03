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
    # RD stage
    "vin_2019stagepcr",
    "new_rdstage2021",
    "new_2023currentrdstage",
    "new_2024currentrdstage",
    "_vin_currentrndstage_value",
    # Include-in-pipeline
    "vin_2019pcrpipelineinclusion",
    "new_includeinpipeline2021",
    "new_2023includeinevgendatabase",
    "new_2024includeinpipeline",
    "new_includeinpipeline",
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

_PIPELINE_TEXT_TO_CODE = {
    "Yes": 100000000,
    "No": 100000001,
    "Pending": 100000001,
}

_TEXT_PIPELINE_COLS = [
    "vin_2019pcrpipelineinclusion",
    "new_2023includeinevgendatabase",
]

# 2024 pipeline column uses a different option set code scheme (862890000 = Yes)
_PIPELINE_CODE_NORMALIZATION = {862890000: 100000000}
_CODE_PIPELINE_COLS = ["new_2024includeinpipeline"]

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


def _normalize_pipeline_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize pipeline columns to consistent integer option set codes.

    Text columns (2019, 2023) are mapped from "Yes"/"No"/"Pending" to codes.
    Code columns with non-standard schemes (2024: 862890000=Yes) are remapped.
    """
    df = df.copy()
    for col in _TEXT_PIPELINE_COLS:
        if col in df.columns:
            df[col] = df[col].map(_PIPELINE_TEXT_TO_CODE)
    for col in _CODE_PIPELINE_COLS:
        if col in df.columns:
            df[col] = df[col].replace(_PIPELINE_CODE_NORMALIZATION)
    return df


def _expand_temporal_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create time-versioned rows via cross-product of temporal groups.

    Handles two temporal groups: RD stage (4 year-columns) and
    includeinpipeline (3 year-columns).  For each candidate the union of
    boundary years from both groups is collected and at each boundary the
    most recent known value for each group is forward-filled.

    Produces one row per candidate per boundary year.
    valid_to = start of candidate's next boundary (None for latest).
    Must be called before column renaming (uses original bronze names).
    """
    _rdstage_cols = [
        ("vin_2019stagepcr", "2019-01-01"),
        ("new_rdstage2021", "2021-01-01"),
        ("new_2023currentrdstage", "2023-01-01"),
        ("new_2024currentrdstage", "2024-01-01"),
        ("_resolved_rdstage_2025", "2025-01-01"),
    ]

    _pipeline_cols = [
        ("vin_2019pcrpipelineinclusion", "2019-01-01"),
        ("new_includeinpipeline2021", "2021-01-01"),
        ("new_2023includeinevgendatabase", "2023-01-01"),
        ("new_2024includeinpipeline", "2024-01-01"),
        ("new_includeinpipeline", "2025-01-01"),
    ]

    def _year_map(row: pd.Series, configs: list[tuple[str, str]]) -> dict:
        """Build {valid_from: value} for non-null year columns."""
        return {
            vf: row[col]
            for col, vf in configs
            if col in row.index and pd.notna(row[col])
        }

    def _forward_fill(year_map: dict, boundaries: list[str]) -> list:
        """At each boundary return the most-recent known value."""
        result = []
        last = None
        for b in boundaries:
            if b in year_map:
                last = year_map[b]
            result.append(last)
        return result

    rows_out: list[dict] = []
    cols_to_drop = _TEMPORAL_SOURCE_COLS + ["_resolved_rdstage_2025"]
    # Columns to carry through (everything except temporal source cols)
    keep_cols = [c for c in df.columns if c not in cols_to_drop]

    for _, row in df.iterrows():
        rd_map = _year_map(row, _rdstage_cols)
        pl_map = _year_map(row, _pipeline_cols)

        boundaries = sorted(set(list(rd_map.keys()) + list(pl_map.keys())))

        if not boundaries:
            out = {c: row[c] for c in keep_cols}
            out["new_currentrdstage"] = None
            out["includeinpipeline"] = None
            out["valid_from"] = None
            out["valid_to"] = None
            rows_out.append(out)
            continue

        rd_vals = _forward_fill(rd_map, boundaries)
        pl_vals = _forward_fill(pl_map, boundaries)

        for i, boundary in enumerate(boundaries):
            out = {c: row[c] for c in keep_cols}
            out["new_currentrdstage"] = rd_vals[i]
            out["includeinpipeline"] = pl_vals[i]
            out["valid_from"] = boundary
            out["valid_to"] = boundaries[i + 1] if i + 1 < len(boundaries) else None
            rows_out.append(out)

    return pd.DataFrame(rows_out)


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
    # 0. Filter deleted records (statecode != 0)
    if "statecode" in df.columns:
        df = df[df["statecode"] == 0].copy()

    # 1. Resolve FK for 2025 RD stage
    if lookup_tables and "vin_rdstageproducts" in lookup_tables:
        df = _resolve_rdstage_fk(df, lookup_tables["vin_rdstageproducts"])

    # 1b. Normalize text-valued pipeline columns to integer codes
    df = _normalize_pipeline_cols(df)

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

    # 7. Derive boolean include_in_pipeline from option set codes
    if "includeinpipeline" in df.columns:
        _pipeline_codes = {100000000}
        df["include_in_pipeline"] = (
            df["includeinpipeline"].isin(_pipeline_codes).astype(int)
        )

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
