"""Diseases table transformation (vin_diseases)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
)

_COLUMNS_TO_DROP = [
    "row_id",
    "createdon",
    "modifiedon",
    "_organizationid_value",
    "crc8b_addedclinicalvalue",
    "crc8b_tppppc",
    "crc8b_addedclinicalvaluedescription",
    "crc8b_p2iproductlaunch",
    "statuscode",
    "statecode",
    "_createdby_value",
    "new_globalhealthareaportal",
    "importsequencenumber",
    "new_incl_eid",
    "_modifiedby_value",
    "new_incl_nd",
    "json_response",
    "sync_time",
]

_COLUMN_RENAMES = {
    "vin_disease": "disease",
    "vin_name": "name",
    "vin_type": "type",
    # New canonical name -- the raw column has a typo ("diseae"). The
    # downstream filter UI consumes this as the secondary-disease label.
    "new_secondary_diseae_choice_text": "secondary_disease_name",
    "vin_diseasecode": "diseasecode",
    "_vin_product_value": "product_value",
    "new_disease_simple": "disease_simple",
    # Renamed from `diseasefilter` so the Silver/Gold name matches the
    # filter-UI semantics: this is the *primary disease filter group*.
    "new_diseasefilter": "disease_filter",
    "new_disease_sort": "diseasesort",
    "new_secondary_disease_filter": "secondary_disease_filter",
    "new_disease_choice_text": "diseasechoice_text",
    "vin_diseaseid": "diseaseid",
    "_vin_maindisease_value": "maindisease_value",
    "new_globalhealtharea": "globalhealtharea",
}

_GLOBAL_HEALTH_AREA_LABEL_UPDATE = {
    "Sexual & reproductive health": "Womens Health",
}


def transform_diseases(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_diseases table from Bronze to Silver.

    Args:
        df: Raw diseases DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames keyed by table name.

    Returns:
        Tuple of (transformed DataFrame, dict of cleaned option sets).
    """
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)
    df = drop_empty_columns(df, preserve=["valid_to"])
    df = rename_columns(df, _COLUMN_RENAMES)

    # =========================================================
    # Disease-filter cleanup (primary + secondary)
    # =========================================================
    #
    # Both columns ride into Gold via `dim_disease` and back the
    # hierarchical filter on Portfolio Analysis / Cross-pipeline
    # Analytics. Bronze data has three quirks the rest of the
    # pipeline does not need to know about:
    #
    #   1. Trailing whitespace on several `new_diseasefilter`
    #      values (e.g. "Malaria ", "Kinetoplastid diseases ").
    #   2. A `"No secondary disease"` sentinel and empty strings
    #      where `IS NULL` would be more useful.
    #   3. A handful of rows storing the primary as a parent-child
    #      concatenation ("STIs - Gonorrhea") rather than the
    #      parent alone.
    #
    # All three are normalized here so downstream consumers see
    # clean, comparable values.

    for col in ("disease_filter", "secondary_disease_name"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Map the "No secondary disease" sentinel and empty strings to
    # NA. After this, a single `IS NULL` test means "this disease
    # has no secondary".
    if "secondary_disease_name" in df.columns:
        s = df["secondary_disease_name"]
        df["secondary_disease_name"] = s.where(
            s.notna() & (s != "") & (s != "No secondary disease"),
            other=pd.NA,
        )

    # STI primary normalization (self-validating).
    #
    # Three rows in the current Bronze sample store the primary as
    # a "<parent> - <child>" concatenation, e.g.
    #     "Sexually transmitted infections (STIs) - Gonorrhea"
    # alongside `secondary_disease_name = "Gonorrhea"`.
    #
    # We collapse the primary to the parent ONLY when the suffix
    # after the first " - " exactly matches the secondary text.
    # This narrow rule is self-validating: any future Dataverse
    # value containing " - " for an unrelated reason stays
    # untouched (the suffix won't match its secondary).
    if "disease_filter" in df.columns and "secondary_disease_name" in df.columns:
        primary = df["disease_filter"]
        secondary = df["secondary_disease_name"]
        has_dash = primary.fillna("").str.contains(" - ", regex=False)
        # Compute the proposed parent (substring before first " - ").
        parent_candidate = primary.str.split(" - ", n=1).str[0]
        suffix_candidate = primary.str.split(" - ", n=1).str[1]
        suffix_matches_secondary = (
            suffix_candidate.notna()
            & secondary.notna()
            & (suffix_candidate == secondary)
        )
        mask = has_dash & suffix_matches_secondary
        df.loc[mask, "disease_filter"] = parent_candidate[mask]

    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets and "_optionset_new_globalhealtharea" in option_sets:
        os_df = option_sets["_optionset_new_globalhealtharea"].copy()
        os_df["label"] = os_df["label"].replace(_GLOBAL_HEALTH_AREA_LABEL_UPDATE)
        cleaned_option_sets["_optionset_new_globalhealtharea"] = os_df

    return df, cleaned_option_sets
