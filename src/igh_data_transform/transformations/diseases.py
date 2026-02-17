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
    "new_secondary_diseae_choice_text": "secondary_diseae_choice_text",
    "vin_diseasecode": "diseasecode",
    "_vin_product_value": "product_value",
    "new_disease_simple": "disease_simple",
    "new_diseasefilter": "diseasefilter",
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

    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets and "_optionset_new_globalhealtharea" in option_sets:
        os_df = option_sets["_optionset_new_globalhealtharea"].copy()
        os_df["label"] = os_df["label"].replace(_GLOBAL_HEALTH_AREA_LABEL_UPDATE)
        cleaned_option_sets["_optionset_new_globalhealtharea"] = os_df

    return df, cleaned_option_sets
