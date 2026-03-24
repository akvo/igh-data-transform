"""Bronze to Silver layer transformation orchestration."""

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd

from igh_data_transform.temporal_backfill import BackfillEngine
from igh_data_transform.transformations.candidates import transform_candidates
from igh_data_transform.transformations.cleanup import (
    drop_empty_columns,
    normalize_whitespace,
    rename_columns,
    replace_values,
)
from igh_data_transform.transformations.clinical_trials import transform_clinical_trials
from igh_data_transform.transformations.diseases import transform_diseases
from igh_data_transform.transformations.priorities import transform_priorities
from igh_data_transform.utils.database import DatabaseManager

# Path to temporal columns report bundled with the package
_TEMPORAL_REPORT_PATH = str(
    Path(__file__).resolve().parent.parent
    / "temporal_backfill"
    / "temporal_columns_report.json"
)

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


def _load_option_sets(
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


def _run_temporal_backfill(bronze_db_path: str) -> str:
    """Run temporal backfill on the raw Bronze DB.

    Consolidates year-specific columns (e.g., new_2023currentrdstage,
    new_2024currentrdstage) into single base columns with proper SCD2
    valid_from/valid_to temporal versioning.

    Args:
        bronze_db_path: Path to raw Bronze database.

    Returns:
        Path to the backfilled intermediate database (temp file).
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()

    engine = BackfillEngine(
        raw_db=bronze_db_path,
        report_path=_TEMPORAL_REPORT_PATH,
        output_db=tmp.name,
    )
    engine.run()

    return tmp.name


def bronze_to_silver(bronze_db_path: str, silver_db_path: str) -> bool:
    """Transform Bronze layer to Silver layer.

    First runs a temporal backfill to consolidate year-specific columns
    into SCD2 temporal versions, then applies table-specific transformers
    and generic cleanup.

    Args:
        bronze_db_path: Path to the Bronze layer SQLite database.
        silver_db_path: Path to the Silver layer SQLite database (will be created).

    Returns:
        True if transformation succeeded, False otherwise.
    """
    backfilled_db_path = None
    try:
        # Phase 0: Temporal backfill — consolidate year-specific columns
        # into base columns with SCD2 valid_from/valid_to versioning.
        backfilled_db_path = _run_temporal_backfill(bronze_db_path)

        # From here on, read from the backfilled DB instead of raw Bronze
        source_db_path = backfilled_db_path

        # Get list of tables
        with DatabaseManager(source_db_path) as source_db:
            cursor = source_db.execute(
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
        source_conn = sqlite3.connect(source_db_path)
        silver_conn = sqlite3.connect(silver_db_path)

        # Track which option sets have been cleaned by transformers
        all_cleaned_option_sets: dict[str, pd.DataFrame] = {}

        # Process data tables
        for table_name in data_tables:
            print(f"Transforming table: {table_name}")

            df = pd.read_sql_query(f"SELECT * FROM {table_name}", source_conn)  # noqa: S608

            if df.empty:
                print(f"  Skipping empty table: {table_name}")
                continue

            if table_name in TABLE_REGISTRY:
                # Dispatch to table-specific transformer
                entry = TABLE_REGISTRY[table_name]
                option_sets = _load_option_sets(source_conn, entry["option_sets"])
                df_transformed, cleaned_os = entry["transformer"](
                    df, option_sets=option_sets,
                )
                all_cleaned_option_sets.update(cleaned_os)
            else:
                # Generic cleanup
                df_transformed = transform_table(
                    df,
                    preserve_columns=["valid_to", "valid_from"],
                )

            # Write transformed table to Silver
            df_transformed.to_sql(table_name, silver_conn, if_exists="replace", index=False)
            print(f"  Wrote {len(df_transformed)} rows to {table_name}")

        # Process option set tables
        for os_table in option_set_tables:
            if os_table in all_cleaned_option_sets:
                # Write the cleaned version from the transformer
                df_os = all_cleaned_option_sets[os_table]
            else:
                # Copy as-is from source
                df_os = pd.read_sql_query(f"SELECT * FROM {os_table}", source_conn)  # noqa: S608

            df_os.to_sql(os_table, silver_conn, if_exists="replace", index=False)
            print(f"  Wrote {len(df_os)} rows to {os_table}")

        source_conn.close()
        silver_conn.close()
        print(f"Bronze to Silver transformation complete: {len(tables)} tables processed")
        return True

    except Exception as e:
        print(f"Error during transformation: {e}")
        return False

    finally:
        # Clean up temp backfilled DB
        if backfilled_db_path:
            try:
                Path(backfilled_db_path).unlink(missing_ok=True)
            except OSError:
                pass
