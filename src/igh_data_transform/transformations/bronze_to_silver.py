"""Bronze to Silver layer transformation orchestration."""

import sqlite3

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_empty_columns,
    normalize_whitespace,
    rename_columns,
    replace_values,
)
from igh_data_transform.utils.database import DatabaseManager


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


def bronze_to_silver(bronze_db_path: str, silver_db_path: str) -> bool:
    """Transform Bronze layer to Silver layer.

    Reads tables from the Bronze database, applies cleanup transformations,
    and writes the results to the Silver database.

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

        # Connect to Silver database (create if not exists)
        silver_conn = sqlite3.connect(silver_db_path)

        # Process each table
        for table_name in tables:
            print(f"Transforming table: {table_name}")

            # Read table from Bronze
            bronze_conn = sqlite3.connect(bronze_db_path)
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", bronze_conn)  # noqa: S608
            bronze_conn.close()

            if df.empty:
                print(f"  Skipping empty table: {table_name}")
                continue

            # Apply standard cleanup transformations
            df_transformed = transform_table(
                df,
                preserve_columns=["valid_to", "valid_from"],
            )

            # Write to Silver database
            df_transformed.to_sql(table_name, silver_conn, if_exists="replace", index=False)
            print(f"  Wrote {len(df_transformed)} rows to {table_name}")

        silver_conn.close()
        print(f"Bronze to Silver transformation complete: {len(tables)} tables processed")
        return True

    except Exception as e:
        print(f"Error during transformation: {e}")
        return False
