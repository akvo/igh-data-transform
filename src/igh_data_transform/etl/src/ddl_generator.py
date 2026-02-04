"""
DDL Generator module for creating target schema from STAR_SCHEMA_MAP.

Generates CREATE TABLE statements by inferring column types from naming conventions:
- *_key, *_id → INTEGER
- *_date → TEXT (ISO format)
- *_flag, *_count → INTEGER
- Everything else → TEXT
"""

import logging
from typing import Any

from config.schema_map import STAR_SCHEMA_MAP, TABLE_LOAD_ORDER

logger = logging.getLogger(__name__)


INTEGER_SUFFIXES = ("_key", "_id", "_flag", "_count")
INTEGER_EXACT_NAMES = {"sort_order", "year", "quarter", "enrollment_count"}


def infer_column_type(column_name: str) -> str:
    """
    Infer SQL column type from column name conventions.

    Args:
        column_name: Name of the column

    Returns:
        SQLite type string (INTEGER or TEXT)
    """
    name_lower = column_name.lower()

    # Integer types by suffix or exact name
    if name_lower.endswith(INTEGER_SUFFIXES) or name_lower in INTEGER_EXACT_NAMES:
        return "INTEGER"

    # Default to TEXT for everything else
    return "TEXT"


def generate_create_table(table_name: str, table_config: dict[str, Any]) -> str:
    """
    Generate CREATE TABLE statement for a single table.

    Args:
        table_name: Name of the target table
        table_config: Configuration dict from STAR_SCHEMA_MAP

    Returns:
        CREATE TABLE SQL statement
    """
    pk_column = table_config.get("_pk")

    # Collect column definitions
    col_defs = []

    # Add primary key first if specified
    if pk_column:
        col_defs.append(f"{pk_column} INTEGER PRIMARY KEY AUTOINCREMENT")

    # Add other columns
    for col_name, source_expr in table_config.items():
        # Skip meta keys
        if col_name.startswith("_"):
            continue
        # Skip primary key (already added)
        if col_name == pk_column:
            continue
        # Skip None values (placeholder columns)
        if source_expr is None:
            continue

        col_type = infer_column_type(col_name)
        col_defs.append(f"{col_name} {col_type}")

    # Generate statement
    columns_sql = ",\n        ".join(col_defs)
    create_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );"""

    return create_sql


def generate_all_ddl() -> list[str]:
    """
    Generate all CREATE TABLE statements in dependency order.

    Returns:
        List of CREATE TABLE SQL statements
    """
    ddl_statements = []

    for table_name in TABLE_LOAD_ORDER:
        if table_name not in STAR_SCHEMA_MAP:
            logger.warning(f"Table {table_name} in load order but not in schema map")
            continue

        table_config = STAR_SCHEMA_MAP[table_name]
        create_sql = generate_create_table(table_name, table_config)
        ddl_statements.append(create_sql)
        logger.debug(f"Generated DDL for {table_name}")

    logger.info(f"Generated {len(ddl_statements)} CREATE TABLE statements")
    return ddl_statements


def print_ddl() -> None:
    """Print all DDL statements to stdout."""
    for stmt in generate_all_ddl():
        print(stmt)
        print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print_ddl()
