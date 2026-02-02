"""Analyze temporal data for entities."""

import sqlite3
from typing import Any


class TemporalAnalyzer:
    """Analyzes temporal data for individual entities."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        temporal_groups: dict[str, list[dict[str, Any]]],
    ):
        """
        Initialize temporal analyzer.

        Args:
            conn: Database connection
            table_name: Name of the table to analyze
            temporal_groups: Dict mapping base_name to list of temporal column info
        """
        self.conn = conn
        self.table_name = table_name
        self.temporal_groups = temporal_groups

    def analyze_entity(
        self, entity_id: Any, business_key_column: str
    ) -> dict[str, dict[int, Any]]:
        """
        Extract temporal data for a single entity.

        Args:
            entity_id: Value of the business key (e.g., vin_id value)
            business_key_column: Name of the business key column (e.g., "vin_id")

        Returns:
            Dict mapping base_name to year-value mapping
            Example:
            {
                "new_knownfunders": {
                    2021: "Funder A",
                    2024: "Funder B",
                    2025: "Funder C"
                },
                "new_developers": {
                    2021: "Dev X",
                    2025: "Dev Y"
                }
            }
        """
        # Query the current record for this entity
        cursor = self.conn.cursor()

        # S608: table_name is from config, not user input
        query = f"SELECT * FROM {self.table_name} WHERE {business_key_column} = ? AND valid_to IS NULL"  # noqa: S608
        cursor.execute(query, (entity_id,))

        row = cursor.fetchone()
        if not row:
            return {}

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Convert row to dict
        row_dict = dict(zip(column_names, row))

        # Extract temporal data by group
        temporal_data: dict[str, dict[int, Any]] = {}

        for base_name, columns in self.temporal_groups.items():
            year_values: dict[int, Any] = {}

            for col_info in columns:
                col_name = col_info["column"]
                year = col_info["year"]

                value = row_dict.get(col_name)

                # Only include non-NULL values
                if value is not None:
                    year_values[year] = value

            if year_values:
                temporal_data[base_name] = year_values

        return temporal_data

    def get_all_entity_ids(self, business_key_column: str) -> list[Any]:
        """
        Get all unique business key values (entities) from the table.

        Args:
            business_key_column: Name of the business key column

        Returns:
            List of unique business key values
        """
        cursor = self.conn.cursor()

        # S608: table_name and business_key_column are from config, not user input
        query = f"SELECT DISTINCT {business_key_column} FROM {self.table_name} WHERE {business_key_column} IS NOT NULL AND valid_to IS NULL ORDER BY {business_key_column}"  # noqa: S608
        cursor.execute(query)

        return [row[0] for row in cursor.fetchall()]

    def get_base_record(
        self, entity_id: Any, business_key_column: str
    ) -> dict[str, Any]:
        """
        Get the base (current) record for an entity with all non-temporal columns.

        Args:
            entity_id: Value of the business key
            business_key_column: Name of the business key column

        Returns:
            Dict of column name to value (excluding temporal columns and SCD2 columns)
        """
        cursor = self.conn.cursor()

        # S608: table_name and business_key_column are from config, not user input
        query = f"SELECT * FROM {self.table_name} WHERE {business_key_column} = ? AND valid_to IS NULL"  # noqa: S608
        cursor.execute(query, (entity_id,))

        row = cursor.fetchone()
        if not row:
            return {}

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Convert row to dict
        row_dict = dict(zip(column_names, row))

        # Build set of temporal columns to exclude
        temporal_columns = set()
        for columns in self.temporal_groups.values():
            for col_info in columns:
                temporal_columns.add(col_info["column"])

        # Exclude temporal columns, row_id, valid_from, valid_to
        exclude_columns = temporal_columns | {"row_id", "valid_from", "valid_to"}

        base_record = {
            k: v for k, v in row_dict.items() if k not in exclude_columns
        }

        return base_record
