"""Priorities table transformation (vin_rdpriorities)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
    replace_values,
)

_COLUMNS_TO_DROP = [
    "row_id",
    "crc8b_includeinipps",
    "_owninguser_value",
    "statecode",
    "_ownerid_value",
    "importsequencenumber",
    "_modifiedby_value",
    "crc8b_addedclinicalvalue",
    "crc8b_p2iproductlaunchbasedonrdpriority",
    "_createdby_value",
    "statuscode",
    "timezoneruleversionnumber",
    "crc8b_realisticlaunch",
    "_owningbusinessunit_value",
    "json_response",
    "sync_time",
]

_COLUMN_RENAMES = {
    "vin_name": "name",
    "new_safety": "safety",
    "new_targetpopulation": "targetpopulation",
    "_vin_disease_value": "diseasevalue",
    "vin_rdpriorityid": "rdpriorityid",
    "new_publicationdate": "publicationdate",
    "new_efficacy": "efficacy",
    "new_indication": "indication",
    "new_intendeduse": "intendeduse",
    "new_source": "source",
    "_vin_secondarydisease_value": "secondarydiseasevalue",
    "new_ppctitle": "ppctitle",
    "_vin_product_value": "product_value",
    "new_author": "author",
}

_AUTHOR_MAPPING = {
    "World Health Organization": "WHO",
}


def transform_priorities(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_rdpriorities table from Bronze to Silver.

    Args:
        df: Raw priorities DataFrame from Bronze layer.
        option_sets: Unused for priorities (no option set updates needed).

    Returns:
        Tuple of (transformed DataFrame, empty dict of cleaned option sets).
    """
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)
    df = drop_empty_columns(df, preserve=["valid_to"])
    df = rename_columns(df, _COLUMN_RENAMES)
    df = replace_values(df, "author", _AUTHOR_MAPPING)
    return df, {}
