"""Transformer module for applying schema mappings to target star schema."""

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from config.phase_sort_order import PHASE_SORT_ORDER
from config.schema_map import STAR_SCHEMA_MAP
from src import bridges
from src.expressions import evaluate_lookup, parse_case_when, parse_coalesce
from src.extractor import Extractor

# Length of ISO date string (YYYY-MM-DD)
ISO_DATE_LENGTH = 10

# Normalize non-standard phase names to their canonical dim_phase equivalents
_PHASE_ALIASES = {
    "Approved product": "Approved",
    "Discovery and Preclinical": "Discovery and preclinical",
    "N/A": "Not applicable",
}

logger = logging.getLogger(__name__)


class Transformer:
    """Transforms source data according to STAR_SCHEMA_MAP."""

    def __init__(self, extractor: Extractor):
        """Initialize transformer with extractor for source data access."""
        self.extractor = extractor
        # Dimension key caches: {table_name: {lookup_value: surrogate_key}}
        self._dim_caches: dict[str, dict[Any, int]] = {}
        # Composite key caches for tech/regulatory dims
        self._composite_caches: dict[str, dict[tuple, int]] = {}
        # Cross-reference maps for resolving FKs via candidate relationship
        self._candidate_cross_refs: dict[str, dict[int, int | None]] = {}

    def _parse_optionset(self, expr: str, row: dict) -> str | None:
        """Parse OPTIONSET:column_name and look up label."""
        col_name = expr[len("OPTIONSET:") :]
        code = row.get(col_name)
        if code is None:
            return None
        return self.extractor.lookup_optionset(col_name, code)

    def _evaluate_expression(self, expr: str, row: dict) -> Any:
        """Evaluate a source expression against a row."""
        # Handle None and specially-handled expressions
        if expr is None or expr.startswith(("FK:", "FK_VIA_CANDIDATE:", "LITERAL:")) or expr == "GENERATED":
            return None

        # Handle special expression types with dispatch
        if expr.startswith("COALESCE("):
            return parse_coalesce(expr, row)
        if expr.upper().startswith("CASE WHEN"):
            return parse_case_when(expr, row)
        if expr.startswith("OPTIONSET:"):
            return self._parse_optionset(expr, row)
        if expr.startswith("LOOKUP:"):
            return evaluate_lookup(expr, row)

        # Simple column reference
        return row.get(expr)

    def cache_dimension_keys(self, table_name: str, data: list[dict], pk_col: str, lookup_col: str) -> None:
        """Cache surrogate keys for a dimension table."""
        self._dim_caches[table_name] = {}
        for row in data:
            pk_val = row.get(pk_col)
            lookup_val = row.get(lookup_col)
            if pk_val is not None and lookup_val is not None:
                self._dim_caches[table_name][lookup_val] = pk_val

        logger.debug(f"Cached {len(self._dim_caches[table_name])} keys for {table_name}")

    def cache_composite_keys(self, table_name: str, data: list[dict], pk_col: str, key_cols: list[str]) -> None:
        """Cache surrogate keys for dimension with composite natural key."""
        self._composite_caches[table_name] = {}
        for row in data:
            pk_val = row.get(pk_col)
            key_tuple = tuple(row.get(col) for col in key_cols)
            if pk_val is not None:
                self._composite_caches[table_name][key_tuple] = pk_val

        logger.debug(f"Cached {len(self._composite_caches[table_name])} composite keys for {table_name}")

    def cache_dimension_keys_by_name(self, table_name: str, data: list[dict], pk_col: str, name_col: str) -> None:
        """Cache surrogate keys by name column for optionset-based lookups."""
        cache_key = f"{table_name}_by_{name_col}"
        self._dim_caches[cache_key] = {}
        for row in data:
            pk_val = row.get(pk_col)
            name_val = row.get(name_col)
            if pk_val is not None and name_val is not None:
                self._dim_caches[cache_key][name_val] = pk_val
        logger.debug(f"Cached {len(self._dim_caches[cache_key])} keys for {cache_key}")

    def lookup_dimension_key(self, table_name: str, lookup_value: Any) -> int | None:
        """Look up surrogate key for a dimension."""
        cache = self._dim_caches.get(table_name, {})
        return cache.get(lookup_value)

    def lookup_composite_key(self, table_name: str, key_values: tuple) -> int | None:
        """Look up surrogate key using composite key."""
        cache = self._composite_caches.get(table_name, {})
        return cache.get(key_values)

    def transform_dimension(self, table_name: str) -> list[dict]:
        """Transform a dimension table according to schema map."""
        config = STAR_SCHEMA_MAP[table_name]
        source_table = config.get("_source_table")
        special = config.get("_special", {})

        # Handle generated dimensions
        if source_table is None and special.get("generate"):
            return self._generate_date_dimension(special)

        # Handle DISTINCT dimensions
        if special.get("distinct"):
            return self._transform_distinct_dimension(table_name, config)

        # Handle dimensions built from optionset cache
        if special.get("from_optionset"):
            return self._transform_optionset_dimension(table_name, config, special)

        # Handle dimensions extracted from delimited fields
        if special.get("extract_distinct_from_delimited"):
            return self._transform_delimited_dimension(table_name, config, special)

        # Standard dimension transformation
        has_fk_lookups = special.get("fk_lookups", False)
        transformed = []
        for row in self.extractor.extract_table(source_table):
            new_row = {}
            for target_col, source_expr in config.items():
                if target_col.startswith("_"):
                    continue
                if has_fk_lookups and source_expr.startswith("FK:"):
                    new_row[target_col] = self._resolve_fk(source_expr, row, "")
                else:
                    new_row[target_col] = self._evaluate_expression(source_expr, row)
            transformed.append(new_row)

        # Inject synthetic phases that exist in PHASE_SORT_ORDER but not in vin_rdstages
        if table_name == "dim_phase":
            transformed = self._inject_synthetic_phases(transformed)

        logger.info(f"Transformed {len(transformed)} rows for {table_name}")
        return transformed

    def _transform_distinct_dimension(self, table_name: str, config: dict) -> list[dict]:
        """
        Transform dimension with DISTINCT handling.
        Creates unique combinations of columns and assigns surrogate keys.
        """
        source_table = config["_source_table"]

        # Map target columns to source columns
        col_mapping = {}
        source_cols = []
        for target_col, source_expr in config.items():
            if target_col.startswith("_"):
                continue
            # Extract actual source column from expression
            if source_expr.startswith("COALESCE("):
                match = re.match(r"COALESCE\((\w+),", source_expr)
                if match:
                    source_cols.append(match.group(1))
                    col_mapping[target_col] = source_expr
            elif source_expr.startswith("OPTIONSET:"):
                source_col = source_expr[len("OPTIONSET:") :]
                source_cols.append(source_col)
                col_mapping[target_col] = source_expr
            else:
                source_cols.append(source_expr)
                col_mapping[target_col] = source_expr

        # Get unique combinations
        seen = set()
        transformed = []
        for row in self.extractor.extract_table(source_table, source_cols):
            # Create tuple of values for deduplication
            key = tuple(row.get(col) for col in source_cols)
            if key in seen:
                continue
            seen.add(key)

            new_row = {}
            for target_col, source_expr in col_mapping.items():
                new_row[target_col] = self._evaluate_expression(source_expr, row)
            transformed.append(new_row)

        logger.info(f"Transformed {len(transformed)} distinct rows for {table_name}")
        return transformed

    def _transform_delimited_dimension(self, table_name: str, config: dict, special: dict) -> list[dict]:
        """Transform dimension by extracting distinct values from a delimited field."""
        source_table = config["_source_table"]
        source_column = special["source_column"]
        delimiter = special["delimiter"]

        # Collect all unique values
        unique_values: set[str] = set()

        for row in self.extractor.extract_table(source_table, [source_column]):
            delimited_value = row.get(source_column)
            if delimited_value:
                # Split by delimiter and trim whitespace
                parts = [p.strip() for p in delimited_value.split(delimiter)]
                for part in parts:
                    if part:  # Skip empty strings
                        unique_values.add(part)

        # Create dimension rows
        transformed = []
        for value in sorted(unique_values):  # Sort for deterministic output
            new_row = {}
            for target_col, source_expr in config.items():
                if target_col.startswith("_"):
                    continue
                if source_expr == "DELIMITED_VALUE":
                    new_row[target_col] = value
                else:
                    new_row[target_col] = self._evaluate_expression(source_expr, {})

            transformed.append(new_row)

        logger.info(f"Transformed {len(transformed)} distinct values from delimited field for {table_name}")
        return transformed

    def _transform_optionset_dimension(self, table_name: str, config: dict, special: dict) -> list[dict]:
        """Transform dimension by reading directly from optionset cache."""
        optionset_name = special["optionset_name"]
        optionset_data = self.extractor._optionset_cache.get(optionset_name, {})

        if not optionset_data:
            logger.warning(f"No optionset data found for {optionset_name}")
            return []

        transformed = []
        for code, label in sorted(optionset_data.items()):
            new_row = {}
            for target_col, source_expr in config.items():
                if target_col.startswith("_"):
                    continue
                if source_expr == "OPTIONSET_LABEL":
                    new_row[target_col] = label
                elif source_expr == "OPTIONSET_CODE":
                    new_row[target_col] = code
            transformed.append(new_row)

        logger.info(f"Transformed {len(transformed)} rows from optionset for {table_name}")
        return transformed

    @staticmethod
    def _generate_date_dimension(special: dict) -> list[dict]:
        """Generate date dimension programmatically."""
        start_year = special.get("start_year", 2015)
        end_year = special.get("end_year", 2030)

        rows = []
        current = date(start_year, 1, 1)
        end = date(end_year + 1, 1, 1)

        while current < end:
            rows.append({
                "full_date": current.isoformat(),
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
            })
            current += timedelta(days=1)

        logger.info(f"Generated {len(rows)} rows for dim_date ({start_year}-{end_year})")
        return rows

    def transform_fact(self, table_name: str) -> list[dict]:
        """Transform a fact table with FK lookups."""
        config = STAR_SCHEMA_MAP[table_name]
        source_table = config["_source_table"]
        today = datetime.now(timezone.utc).date().isoformat()

        transformed = []
        for row in self.extractor.extract_table(source_table):
            new_row = {}
            for target_col, source_expr in config.items():
                if target_col.startswith("_"):
                    continue

                # Skip columns with None expressions (not in target schema)
                if source_expr is None:
                    continue

                if source_expr.startswith("FK_VIA_CANDIDATE:"):
                    # Resolve via candidate cross-reference
                    new_row[target_col] = self._resolve_fk_via_candidate(source_expr, new_row)
                elif source_expr.startswith("FK:"):
                    # Parse FK expression
                    new_row[target_col] = self._resolve_fk(source_expr, row, today)
                else:
                    new_row[target_col] = self._evaluate_expression(source_expr, row)

            transformed.append(new_row)

        logger.info(f"Transformed {len(transformed)} rows for {table_name}")
        return transformed

    def _resolve_fk(self, fk_expr: str, row: dict, today: str) -> int | None:
        """Resolve FK expression like FK:dim_table.lookup_col|source_col."""
        # Parse FK expression
        parts = fk_expr[3:].split("|")  # Remove "FK:" prefix
        dim_ref = parts[0]  # e.g., "dim_candidate_core.vin_candidateid"
        source_ref = parts[1] if len(parts) > 1 else None

        dim_table, lookup_col = dim_ref.split(".")

        # Handle special source references
        if source_ref == "TODAY":
            lookup_val = today
        elif source_ref and source_ref.startswith("EXTRACT_PHASE:"):
            # Extract phase name from combined field like "Phase I - Drugs"
            source_col = source_ref[len("EXTRACT_PHASE:") :]
            combined = row.get(source_col, "")
            if combined:
                # Extract phase part (before " - ")
                phase_part = combined.split(" - ")[0] if " - " in combined else combined
                # Normalize known aliases
                phase_part = _PHASE_ALIASES.get(phase_part, phase_part)
                # Look up by phase name
                lookup_val = self._find_phase_id_by_name(phase_part)
            else:
                lookup_val = None
        elif source_ref and source_ref.startswith("EXTRACT_DATE:"):
            # Extract date from ISO timestamp (e.g., "2015-01-01T00:00:00Z" -> "2015-01-01")
            source_col = source_ref[len("EXTRACT_DATE:") :]
            timestamp = row.get(source_col, "")
            lookup_val = (
                (timestamp[:ISO_DATE_LENGTH] if len(timestamp) >= ISO_DATE_LENGTH else None) if timestamp else None
            )
        elif lookup_col == "COMPOSITE":
            # Resolve composite FK by evaluating the dimension's own expressions
            dim_config = STAR_SCHEMA_MAP.get(dim_table, {})
            dim_special = dim_config.get("_special", {})
            dim_distinct_cols = dim_special.get("distinct_cols", [])
            key_values = []
            for target_col in dim_distinct_cols:
                dim_expr = dim_config.get(target_col)
                key_values.append(self._evaluate_expression(dim_expr, row))
            return self.lookup_composite_key(dim_table, tuple(key_values))
        else:
            lookup_val = row.get(source_ref)

        if lookup_val is None:
            return None

        return self.lookup_dimension_key(dim_table, lookup_val)

    @staticmethod
    def _find_phase_id_by_name(phase_name: str) -> str | None:
        """Find phase ID by name (returns name for lookup)."""
        return phase_name

    @staticmethod
    def _inject_synthetic_phases(transformed: list[dict]) -> list[dict]:
        """Add phases from PHASE_SORT_ORDER that aren't already in the source data."""
        existing_names = {row["phase_name"] for row in transformed if row.get("phase_name")}
        for phase_name, sort_order in PHASE_SORT_ORDER.items():
            if phase_name not in existing_names:
                transformed.append({
                    "vin_rdstageid": None,
                    "phase_name": phase_name,
                    "sort_order": sort_order,
                })
                logger.info(f"Injected synthetic phase: {phase_name}")
        return transformed

    def build_candidate_cross_refs(self, pipeline_data: list[dict]) -> None:
        """Build candidate_key → disease_key/product_key maps from loaded pipeline snapshots."""
        for target_col in ("disease_key", "product_key"):
            self._candidate_cross_refs[target_col] = {}
            for row in pipeline_data:
                ck = row.get("candidate_key")
                val = row.get(target_col)
                if ck is not None:
                    self._candidate_cross_refs[target_col][ck] = val

    def _resolve_fk_via_candidate(self, expr: str, new_row: dict) -> int | None:
        """Resolve FK by looking up the candidate's disease_key or product_key."""
        target_col = expr[len("FK_VIA_CANDIDATE:") :]  # e.g. "disease_key"
        # candidate_key should already be resolved in new_row
        candidate_key = new_row.get("candidate_key")
        if candidate_key is None:
            return None
        cross_ref = self._candidate_cross_refs.get(target_col, {})
        return cross_ref.get(candidate_key)

    def transform_bridge(self, table_name: str) -> list[dict]:
        """Transform a bridge table."""
        return bridges.transform_bridge(self, table_name)
