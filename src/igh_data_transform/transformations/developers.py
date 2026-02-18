"""Developers table transformation (vin_developers)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
)

_COLUMNS_TO_DROP = [
    "row_id",
    "_organizationid_value",
    "utcconversiontimezonecode",
    "importsequencenumber",
    "_modifiedonbehalfby_value",
    "_createdonbehalfby_value",
    "versionnumber",
    "_new_developerid_value",
    "_modifiedby_value",
    "statuscode",
    "overriddencreatedon",
    "_createdby_value",
    "timezoneruleversionnumber",
    "json_response",
    "sync_time",
]

_COLUMN_RENAMES = {
    "vin_developerid": "developerid",
    "vin_name": "developer_product_name",
    "_vin_cap_value": "candidateid",
    "_vin_developer_value": "accountid",
    "vin_organisationtype": "org_type",
}

# Source values in accounts.address1_country that don't match vin_countries.vin_name.
_COUNTRY_ALIASES = {
    "Türkiye": "Turkey",
    "United States": "United States of America",
    "Korea": "South Korea",
    "Korea, Republic of": "South Korea",
    "The Netherlands": "Netherlands",
    "United Kindgom": "United Kingdom",  # typo in source data
    "Macedonia": "North Macedonia",
    "Northern Ireland": "United Kingdom",
    "Not specified": None,
}


def _enrich_from_accounts(
    df: pd.DataFrame,
    accounts: pd.DataFrame,
    countries: pd.DataFrame,
) -> pd.DataFrame:
    """Join developer rows with account and country data.

    1. Left-join accounts on _vin_developer_value = accountid -> org_name,
       vin_organisationtype, address1_country
    2. Resolve address1_country to country_name via vin_countries
    3. Null out numeric-only org type values (unresolvable codes)
    """
    df = df.copy()

    # --- Join accounts ---
    acct = accounts[
        ["accountid", "name", "vin_organisationtype", "address1_country"]
    ].copy()
    acct = acct.rename(columns={"name": "org_name"})
    df = df.merge(
        acct, left_on="_vin_developer_value", right_on="accountid", how="left"
    )
    # Drop the joined accountid (we already have _vin_developer_value)
    df = df.drop(columns=["accountid"])

    # --- Resolve country ---
    # Build case-insensitive text lookup: lowered vin_name -> canonical vin_name
    country_text = dict(
        zip(countries["vin_name"].str.lower(), countries["vin_name"], strict=False)
    )
    # Build numeric lookup: vin_countryno -> canonical vin_name
    country_num = {}
    if "vin_countryno" in countries.columns:
        for _, row in countries.iterrows():
            if pd.notna(row["vin_countryno"]):
                country_num[int(row["vin_countryno"])] = row["vin_name"]

    def _resolve_country(raw):
        if pd.isna(raw):
            return None
        raw_str = str(raw).strip()
        if not raw_str:
            return None

        # Apply alias first
        aliased = _COUNTRY_ALIASES.get(raw_str, raw_str)
        if aliased is None:
            return None

        # Try text match (case-insensitive)
        match = country_text.get(aliased.lower())
        if match:
            return match

        # Try numeric match
        try:
            code = int(float(raw_str))
            return country_num.get(code)
        except (ValueError, OverflowError):
            return None

    df["country_name"] = df["address1_country"].apply(_resolve_country)
    df = df.drop(columns=["address1_country"])

    # --- Clean org type: null out purely numeric values ---
    if "vin_organisationtype" in df.columns:
        mask = df["vin_organisationtype"].apply(
            lambda v: _is_numeric_string(v) if pd.notna(v) else False
        )
        df.loc[mask, "vin_organisationtype"] = None

    return df


def _is_numeric_string(value) -> bool:
    """Return True if value is a purely numeric string (e.g. '5001')."""
    try:
        int(float(value))
        return str(value).strip().replace(".", "").replace("-", "").isdigit()
    except (ValueError, TypeError):
        return False


def transform_developers(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
    lookup_tables: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_developers table from Bronze to Silver.

    Args:
        df: Raw developers DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames (unused for developers).
        lookup_tables: Dict with 'accounts' and 'vin_countries' DataFrames.

    Returns:
        Tuple of (transformed DataFrame, empty dict of cleaned option sets).
    """
    # 1. Enrich from accounts + countries
    if lookup_tables:
        accounts = lookup_tables.get("accounts")
        countries = lookup_tables.get("vin_countries")
        if accounts is not None and countries is not None:
            df = _enrich_from_accounts(df, accounts, countries)

    # 2. Drop metadata columns
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)

    # 3. Drop empty columns (preserve valid_to)
    df = drop_empty_columns(df, preserve=["valid_to"])

    # 4. Rename columns
    df = rename_columns(df, _COLUMN_RENAMES)

    return df, {}
