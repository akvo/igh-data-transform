"""Transform table schemas to replace temporal columns with base columns."""

import re
from typing import Any


class SchemaTransformer:
    """Transforms table schema to use base columns instead of temporal columns."""

    def __init__(self, temporal_groups: dict[str, list[dict[str, Any]]]):
        """
        Initialize schema transformer.

        Args:
            temporal_groups: Dict mapping base_name to list of temporal column info
                Example: {"new_knownfunders": [{"column": "new_knownfunders2021", "year": 2021}, ...]}
        """
        self.temporal_groups = temporal_groups

        # Build set of all temporal column names for quick lookup
        self.temporal_columns = set()
        for columns in temporal_groups.values():
            for col_info in columns:
                self.temporal_columns.add(col_info["column"])

    def transform_schema(self, original_schema: str) -> str:
        """
        Transform CREATE TABLE statement.

        Args:
            original_schema: Original CREATE TABLE SQL

        Returns:
            Transformed CREATE TABLE SQL with temporal columns replaced by base columns
        """
        # Parse the CREATE TABLE statement
        lines = original_schema.strip().split("\n")

        # Extract table name from first line
        first_line = lines[0]
        table_match = re.match(r"CREATE TABLE (\w+) \(", first_line)
        if not table_match:
            raise ValueError(f"Could not parse table name from: {first_line}")

        table_name = table_match.group(1)

        # Parse column definitions
        column_lines = []
        for line in lines[1:]:
            line = line.strip()
            if line == ")":
                break
            if line.endswith(","):
                line = line[:-1]  # Remove trailing comma

            column_lines.append(line)

        # First pass: collect all existing column names
        existing_columns = set()
        for col_line in column_lines:
            parts = col_line.split()
            if parts:
                existing_columns.add(parts[0])

        # Filter out temporal columns and collect types
        base_column_types = {}
        kept_columns = []

        # Build set of base column names
        base_column_names = set(self.temporal_groups.keys())

        for col_line in column_lines:
            # Extract column name (first word before space)
            parts = col_line.split()
            if not parts:
                continue

            col_name = parts[0]

            # Check if this is a temporal column
            if col_name in self.temporal_columns:
                # Track the type for the base column
                base_name = self._get_base_name(col_name)
                if base_name and base_name not in base_column_types:
                    # Extract type (second word typically)
                    if len(parts) >= 2:
                        col_type = parts[1]
                        base_column_types[base_name] = col_type

                # If this temporal column IS also a base column name,
                # keep it (don't remove it) - it's the current-year column
                # that will serve as the consolidated base column
                if col_name in base_column_names:
                    kept_columns.append(col_line)
                # Otherwise, skip this column (will add base column later)
                continue

            kept_columns.append(col_line)

        # Add base columns (only if they don't already exist)
        for base_name in sorted(self.temporal_groups.keys()):
            # Skip if base column already exists in the table
            if base_name in existing_columns:
                continue

            col_type = base_column_types.get(base_name, "TEXT")  # Default to TEXT
            kept_columns.append(f"  {base_name} {col_type}")

        # Reconstruct CREATE TABLE
        result = [f"CREATE TABLE {table_name} ("]
        for i, col_line in enumerate(kept_columns):
            if i < len(kept_columns) - 1:
                result.append(col_line + ",")
            else:
                result.append(col_line)
        result.append(")")

        return "\n".join(result)

    def _get_base_name(self, temporal_column: str) -> str | None:
        """
        Get base name for a temporal column.

        Args:
            temporal_column: Temporal column name (e.g., "new_knownfunders2021")

        Returns:
            Base name (e.g., "new_knownfunders") or None if not found
        """
        for base_name, columns in self.temporal_groups.items():
            for col_info in columns:
                if col_info["column"] == temporal_column:
                    return base_name
        return None

    def get_base_columns(self) -> list[str]:
        """
        Get list of base column names that will be created.

        Returns:
            List of base column names
        """
        return sorted(self.temporal_groups.keys())
