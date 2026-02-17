"""
Extractor module for reading from the source Dataverse database.

This module provides read-only access to the source database and handles
optionset lookups for converting integer codes to human-readable labels.
"""

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path

logger = logging.getLogger(__name__)


class Extractor:
    """Read-only extractor for the source Dataverse database."""

    def __init__(self, db_path: str | Path):
        """Initialize extractor with path to source database."""
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            msg = f"Source database not found: {self.db_path}"
            raise FileNotFoundError(msg)

        self._conn = None
        self._optionset_cache: dict[str, dict[int, str]] = {}

    def connect(self) -> None:
        """Open read-only connection to source database."""
        uri = f"file:{self.db_path}?mode=ro"
        self._conn = sqlite3.connect(uri, uri=True)
        self._conn.row_factory = sqlite3.Row
        logger.info(f"Connected to source database: {self.db_path}")

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Closed source database connection")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_cursor(self) -> sqlite3.Cursor:
        """Get cursor, ensuring connection is open."""
        if not self._conn:
            msg = "Database not connected. Call connect() first."
            raise RuntimeError(msg)
        return self._conn.cursor()

    def build_optionset_cache(self) -> None:
        """
        Build cache of all optionset tables for fast lookups.

        Optionset tables have format: _optionset_{column_name}
        with columns: code (INTEGER), label (TEXT), timestamp (TEXT)
        """
        cursor = self._get_cursor()

        # Find all optionset tables
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name LIKE '_optionset_%'
        """)
        optionset_tables = [row[0] for row in cursor.fetchall()]

        for table_name in optionset_tables:
            # Extract the column name from table name (after '_optionset_')
            col_name = table_name[len("_optionset_") :]

            try:
                # Optionset tables have format: code|label|timestamp
                # First column is code (int), second is label (text)
                cursor.execute(f"SELECT * FROM [{table_name}]")  # noqa: S608 - table_name from sqlite_master, not user input
                self._optionset_cache[col_name] = {}

                for row in cursor.fetchall():
                    code = row[0]
                    label = row[1] if len(row) > 1 else str(code)
                    if code is not None:
                        self._optionset_cache[col_name][code] = label

            except sqlite3.Error as e:
                logger.warning(f"Could not load optionset {table_name}: {e}")

        logger.info(f"Loaded {len(self._optionset_cache)} optionset tables")

    def lookup_optionset(self, column_name: str, code: int | str | None) -> str | None:
        """
        Look up label for an optionset code.

        Args:
            column_name: The column name (without _optionset_ prefix)
            code: The code to look up (may be int or string representation)

        Returns:
            The human-readable label, or None if not found
        """
        if code is None:
            return None

        if column_name not in self._optionset_cache:
            logger.warning(f"Optionset not found: {column_name}")
            return None

        cache = self._optionset_cache[column_name]

        # Try direct lookup first
        if code in cache:
            return cache[code]

        # If code is a string, try converting to int for lookup
        if isinstance(code, str):
            try:
                int_code = int(code)
                if int_code in cache:
                    return cache[int_code]
            except ValueError:
                pass

        return None

    def get_table_columns(self, table_name: str) -> list[str]:
        """Get list of column names for a table."""
        cursor = self._get_cursor()
        cursor.execute(f"PRAGMA table_info([{table_name}])")
        return [row[1] for row in cursor.fetchall()]

    def extract_table(self, table_name: str, columns: list[str] | None = None) -> Iterator[dict]:
        """
        Extract all rows from a table.

        Args:
            table_name: Name of table to extract
            columns: Optional list of columns to select. If None, selects all.

        Yields:
            Dict for each row with column names as keys
        """
        cursor = self._get_cursor()

        # S608: table_name and columns come from STAR_SCHEMA_MAP config, not user input
        if columns:
            col_list = ", ".join(f"[{c}]" for c in columns)
            sql = f"SELECT {col_list} FROM [{table_name}]"  # noqa: S608
        else:
            sql = f"SELECT * FROM [{table_name}]"  # noqa: S608

        logger.debug(f"Extracting from {table_name}: {sql}")
        cursor.execute(sql)

        for row in cursor:
            yield dict(row)

    def extract_distinct(self, table_name: str, columns: list[str]) -> Iterator[dict]:
        """
        Extract distinct combinations of columns from a table.

        Args:
            table_name: Name of table to extract from
            columns: List of columns to get distinct combinations of

        Yields:
            Dict for each unique combination
        """
        cursor = self._get_cursor()
        col_list = ", ".join(f"[{c}]" for c in columns)
        # S608: table_name and columns come from STAR_SCHEMA_MAP config, not user input
        sql = f"SELECT DISTINCT {col_list} FROM [{table_name}]"  # noqa: S608

        logger.debug(f"Extracting distinct from {table_name}: {sql}")
        cursor.execute(sql)

        for row in cursor:
            yield dict(row)

    def get_last_sync_date(self) -> str | None:
        """Get the end_time of the most recent completed sync with actual data changes."""
        cursor = self._get_cursor()
        cursor.execute("""
            SELECT end_time FROM _sync_log
            WHERE status = 'completed'
              AND (records_added > 0 OR records_updated > 0)
            ORDER BY end_time DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        return row[0] if row else None

    def count_rows(self, table_name: str) -> int:
        """Get row count for a table."""
        cursor = self._get_cursor()
        # S608: table_name from STAR_SCHEMA_MAP config, not user input
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")  # noqa: S608
        return cursor.fetchone()[0]

    def execute_query(self, sql: str) -> Iterator[dict]:
        """
        Execute arbitrary SELECT query.

        Args:
            sql: SQL query to execute

        Yields:
            Dict for each row
        """
        cursor = self._get_cursor()
        cursor.execute(sql)
        for row in cursor:
            yield dict(row)
