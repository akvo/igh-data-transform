"""Generate SCD2 versions from temporal data."""

from typing import Any

# Sentinel year value meaning "current year" (no explicit year in column name)
CURRENT_YEAR_SENTINEL = 9999

# Default year to use for current-year columns (data is from 2025)
DEFAULT_CURRENT_YEAR = 2025


class SCD2Generator:
    """Generates SCD2 versions from temporal data."""

    def __init__(
        self,
        current_year: int | None = None,
        all_base_columns: set[str] | None = None,
    ):
        """
        Initialize SCD2 generator.

        Args:
            current_year: Year to use for current-year columns (columns without
                explicit year suffix). Defaults to 2025.
            all_base_columns: Set of all base column names. If provided, every
                version will include all these columns (NULL for missing data).
        """
        self.current_year = current_year or DEFAULT_CURRENT_YEAR
        self.all_base_columns = all_base_columns or set()

    def generate_versions(
        self,
        entity_id: Any,
        business_key_column: str,
        base_record: dict[str, Any],
        temporal_data: dict[str, dict[int, Any]],
    ) -> list[dict[str, Any]]:
        """
        Generate SCD2 versions from temporal data.

        Creates versions at each unique temporal boundary, handling multiple
        temporal groups with a cross-product approach.

        Args:
            entity_id: Value of the business key
            business_key_column: Name of the business key column
            base_record: Base record data (non-temporal columns)
            temporal_data: Temporal data by group
                Example: {"new_knownfunders": {2021: "A", 2024: "B"}}

        Returns:
            List of complete record dicts ready to INSERT
        """
        if not temporal_data:
            # No temporal data - create single version with NULL base columns
            version = base_record.copy()
            version[business_key_column] = entity_id
            version["valid_from"] = None  # Will use current timestamp
            version["valid_to"] = None

            # Add NULL for all base columns
            for base_name in self.all_base_columns:
                version[base_name] = None

            return [version]

        # Convert sentinel year 9999 to actual current year
        temporal_data = self._normalize_current_year(temporal_data, self.current_year)

        # Find all unique years across all temporal groups
        all_years = set()
        for year_values in temporal_data.values():
            all_years.update(year_values.keys())

        # Sort years chronologically
        sorted_years = sorted(all_years)

        # For each temporal group, build a timeline of values
        # group_timeline[base_name][year] = value
        group_timelines: dict[str, dict[int, Any]] = {}

        for base_name, year_values in temporal_data.items():
            timeline: dict[int, Any] = {}
            sorted_group_years = sorted(year_values.keys())

            # For each boundary year, determine the active value
            for boundary_year in sorted_years:
                # Find the most recent year <= boundary_year with data
                active_value = None
                for data_year in sorted_group_years:
                    if data_year <= boundary_year:
                        active_value = year_values[data_year]
                    else:
                        break

                if active_value is not None:
                    timeline[boundary_year] = active_value

            group_timelines[base_name] = timeline

        # Generate versions at each boundary
        versions = []

        for i, year in enumerate(sorted_years):
            # Calculate valid_from
            valid_from = f"{year}-01-01T00:00:00Z"

            # Calculate valid_to
            if i < len(sorted_years) - 1:
                next_year = sorted_years[i + 1]
                valid_to = f"{next_year}-01-01T00:00:00Z"
            else:
                # Last year - still current
                valid_to = None

            # Build version record
            version = base_record.copy()
            version[business_key_column] = entity_id
            version["valid_from"] = valid_from
            version["valid_to"] = valid_to

            # Add values for each base column from temporal data
            for base_name, timeline in group_timelines.items():
                version[base_name] = timeline.get(year)

            # Add NULL for any base columns that don't have temporal data
            for base_name in self.all_base_columns:
                if base_name not in version:
                    version[base_name] = None

            versions.append(version)

        return versions

    def _normalize_current_year(
        self,
        temporal_data: dict[str, dict[int, Any]],
        current_year: int,
    ) -> dict[str, dict[int, Any]]:
        """
        Convert year sentinel 9999 to actual current year.

        If 9999 is present in any group, it represents data from a column
        without an explicit year (the "current" value). Convert it to the
        actual current year.

        If current_year already exists for a group, the 9999 value takes
        precedence (it's the most recent data).

        Args:
            temporal_data: Temporal data with potential 9999 sentinel
            current_year: The actual current year to use

        Returns:
            Normalized temporal data with 9999 converted to current_year
        """
        normalized: dict[str, dict[int, Any]] = {}

        for base_name, year_values in temporal_data.items():
            normalized_years: dict[int, Any] = {}

            for year, value in year_values.items():
                if year == CURRENT_YEAR_SENTINEL:
                    # Convert sentinel to actual current year
                    # This will override any existing current_year value
                    normalized_years[current_year] = value
                else:
                    # Only keep non-current-year values if they're not being
                    # overridden by the sentinel
                    if year != current_year or CURRENT_YEAR_SENTINEL not in year_values:
                        normalized_years[year] = value

            normalized[base_name] = normalized_years

        return normalized

    @staticmethod
    def format_version_summary(versions: list[dict[str, Any]]) -> str:
        """
        Format a summary of versions for logging.

        Args:
            versions: List of version records

        Returns:
            Human-readable summary
        """
        if not versions:
            return "0 versions"

        if len(versions) == 1:
            return "1 version"

        # Extract years from valid_from
        years = []
        for v in versions:
            valid_from = v.get("valid_from")
            if valid_from:
                year = valid_from.split("-")[0]
                years.append(year)

        if years:
            return f"{len(versions)} versions ({', '.join(years)})"
        else:
            return f"{len(versions)} versions"
