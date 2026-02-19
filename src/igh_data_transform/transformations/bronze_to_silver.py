"""Bronze to Silver layer transformation orchestration."""

import sqlite3

import pandas as pd

from igh_data_transform.transformations.candidates import transform_candidates
from igh_data_transform.transformations.cleanup import (
    drop_empty_columns,
    normalize_whitespace,
    rename_columns,
    replace_values,
)
from igh_data_transform.transformations.developers import transform_developers
from igh_data_transform.transformations.clinical_trials import transform_clinical_trials
from igh_data_transform.transformations.diseases import transform_diseases
from igh_data_transform.transformations.priorities import transform_priorities
from igh_data_transform.utils.database import DatabaseManager

# Optionset tables renamed to match silver column names.
# Derived from _COLUMN_RENAMES in each table-specific transformer.
OPTIONSET_RENAMES = {
    # From candidates.py
    "_optionset_vin_approvalstatus": "_optionset_approvalstatus",
    "_optionset_vin_developmentstatus": "_optionset_developmentstatus",
    "_optionset_vin_whoprequalification": "_optionset_whoprequalification",
    "_optionset_vin_nationalregulatoryauthorityapprovalstatus": "_optionset_NRAapprovalstatus",
    "_optionset_new_indicationtype": "_optionset_indicationtype",
    "_optionset_vin_preclinicalresultsstatus": "_optionset_preclinicalresultsstatus",
    "_optionset_new_agespecific": "_optionset_agespecific",
    "_optionset_vin_approvingauthority": "_optionset_approvingauthority",
    "_optionset_vin_stringentregulatoryauthorityapproval": "_optionset_SRA_approvalstatus",
    # From clinical_trials.py
    "_optionset_vin_ctstatus": "_optionset_ctstatus",
    # From diseases.py
    "_optionset_new_globalhealtharea": "_optionset_globalhealtharea",
}

# Silver column names that store optionset codes (derived from OPTIONSET_RENAMES)
_SILVER_OPTIONSET_COLUMNS = {
    v[len("_optionset_") :] for v in OPTIONSET_RENAMES.values()
}

# Registry mapping table names to their specific transformer and required option sets.
TABLE_REGISTRY: dict[str, dict] = {
    "vin_candidates": {
        "transformer": transform_candidates,
        "option_sets": [
            "_optionset_new_indicationtype",
            "_optionset_vin_preclinicalresultsstatus",
            "_optionset_vin_approvalstatus",
            "_optionset_vin_approvingauthority",
        ],
        "lookup_tables": ["vin_rdstageproducts"],
    },
    "vin_clinicaltrials": {
        "transformer": transform_clinical_trials,
        "option_sets": ["_optionset_vin_ctstatus"],
    },
    "vin_diseases": {
        "transformer": transform_diseases,
        "option_sets": ["_optionset_new_globalhealtharea"],
    },
    "vin_rdpriorities": {
        "transformer": transform_priorities,
        "option_sets": [],
    },
    "vin_developers": {
        "transformer": transform_developers,
        "option_sets": [],
        "lookup_tables": ["accounts", "vin_countries"],
    },
}


def transform_table(
    df: pd.DataFrame,
    column_renames: dict[str, str] | None = None,
    value_mappings: dict[str, dict] | None = None,
    text_columns: list[str] | None = None,
    preserve_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Apply standard cleanup transformations to a DataFrame.

    Args:
        df: Input DataFrame from Bronze layer
        column_renames: Dict of {old_name: new_name} for column renaming
        value_mappings: Dict of {column: {old_val: new_val}} for value replacement
        text_columns: List of column names to normalize whitespace
        preserve_columns: Columns to preserve even if empty (e.g., ['valid_to'])

    Returns:
        Transformed DataFrame ready for Silver layer
    """
    # Step 1: Drop empty columns
    preserve = preserve_columns or ["valid_to"]
    df = drop_empty_columns(df, preserve=preserve)

    # Step 2: Rename columns
    if column_renames:
        df = rename_columns(df, column_renames)

    # Step 3: Normalize whitespace in text columns
    if text_columns:
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(normalize_whitespace)

    # Step 4: Replace/consolidate values
    if value_mappings:
        for column, mapping in value_mappings.items():
            if column in df.columns:
                df = replace_values(df, column, mapping)

    return df


def _load_tables(
    bronze_conn: sqlite3.Connection,
    option_set_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Load option set tables from the Bronze database.

    Args:
        bronze_conn: Open connection to Bronze database.
        option_set_names: List of option set table names to load.

    Returns:
        Dict mapping option set table name to DataFrame.
    """
    option_sets: dict[str, pd.DataFrame] = {}
    for name in option_set_names:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {name}", bronze_conn)  # noqa: S608
            option_sets[name] = df
        except Exception:
            pass  # Option set table may not exist in this Bronze DB
    return option_sets


def bronze_to_silver(bronze_db_path: str, silver_db_path: str) -> bool:
    """Transform Bronze layer to Silver layer.

    Reads tables from the Bronze database, applies cleanup transformations,
    and writes the results to the Silver database. Registered tables are
    dispatched to their table-specific transformers; all other tables
    receive generic cleanup.

    Args:
        bronze_db_path: Path to the Bronze layer SQLite database.
        silver_db_path: Path to the Silver layer SQLite database (will be created).

    Returns:
        True if transformation succeeded, False otherwise.
    """
    try:
        # Get list of tables from Bronze database
        with DatabaseManager(bronze_db_path) as bronze_db:
            cursor = bronze_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row["name"] for row in cursor.fetchall()]

        if not tables:
            print("No tables found in Bronze database")
            return True

        # Separate option set tables from data tables
        option_set_tables = {t for t in tables if t.startswith("_optionset_")}
        data_tables = [t for t in tables if t not in option_set_tables]

        # Connect to databases
        bronze_conn = sqlite3.connect(bronze_db_path)
        silver_conn = sqlite3.connect(silver_db_path)

        # Track which option sets have been cleaned by transformers
        all_cleaned_option_sets: dict[str, pd.DataFrame] = {}

        # Process data tables
        for table_name in data_tables:
            print(f"Transforming table: {table_name}")

            df = pd.read_sql_query(f"SELECT * FROM {table_name}", bronze_conn)  # noqa: S608

            if df.empty:
                print(f"  Skipping empty table: {table_name}")
                continue

            if table_name in TABLE_REGISTRY:
                # Dispatch to table-specific transformer
                entry = TABLE_REGISTRY[table_name]
                option_sets = _load_tables(bronze_conn, entry["option_sets"])
                kwargs = {"option_sets": option_sets}
                if "lookup_tables" in entry:
                    kwargs["lookup_tables"] = _load_tables(
                        bronze_conn, entry["lookup_tables"]
                    )
                df_transformed, cleaned_os = entry["transformer"](df, **kwargs)
                all_cleaned_option_sets.update(cleaned_os)

                # Cast optionset code columns to nullable integer
                for col in df_transformed.columns:
                    if col in _SILVER_OPTIONSET_COLUMNS:
                        df_transformed[col] = df_transformed[col].astype("Int64")
            else:
                # Generic cleanup
                df_transformed = transform_table(
                    df,
                    preserve_columns=["valid_to", "valid_from"],
                )

            # Write transformed table to Silver
            df_transformed.to_sql(
                table_name, silver_conn, if_exists="replace", index=False
            )
            print(f"  Wrote {len(df_transformed)} rows to {table_name}")

        # Process option set tables (with renames to match silver column names)
        for os_table in option_set_tables:
            silver_name = OPTIONSET_RENAMES.get(os_table, os_table)
            if os_table in all_cleaned_option_sets:
                # Write the cleaned version from the transformer
                df_os = all_cleaned_option_sets[os_table]
            else:
                # Copy as-is from Bronze
                df_os = pd.read_sql_query(f"SELECT * FROM {os_table}", bronze_conn)  # noqa: S608

            df_os.to_sql(silver_name, silver_conn, if_exists="replace", index=False)
            print(f"  Wrote {len(df_os)} rows to {silver_name}")

        bronze_conn.close()
        silver_conn.close()
        print(
            f"Bronze to Silver transformation complete: {len(tables)} tables processed"
        )
        return True

    except Exception as e:
        print(f"Error during transformation: {e}")
        return False
