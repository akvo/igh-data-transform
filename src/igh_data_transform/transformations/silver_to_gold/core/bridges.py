"""
Bridge transformation functions for transformer.

Handles transformation of bridge tables including:
- Standard bridges
- Union bridges (multiple source tables)
- Delimited bridges (from delimited fields)
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from igh_data_transform.utils.country_aliases import COUNTRY_ALIASES
from igh_data_transform.transformations.silver_to_gold.config.schema_map import STAR_SCHEMA_MAP

if TYPE_CHECKING:
    from igh_data_transform.transformations.silver_to_gold.core.transformer import Transformer

logger = logging.getLogger(__name__)


def transform_bridge(transformer: Transformer, table_name: str) -> list[dict]:
    """
    Transform a bridge table.

    Args:
        transformer: Transformer instance for caches and evaluation
        table_name: Target bridge table name

    Returns:
        List of transformed rows
    """
    config = STAR_SCHEMA_MAP[table_name]
    source_table = config.get("_source_table")
    special = config.get("_special", {})

    if source_table == "UNION":
        return transform_union_bridge(transformer, table_name, config, special)

    # Handle bridge from delimited field
    if special.get("bridge_from_delimited"):
        return transform_delimited_bridge(transformer, table_name, config, special)

    # Standard bridge transformation
    transformed = []
    for row in transformer.extractor.extract_table(source_table):
        new_row = {}
        for target_col, source_expr in config.items():
            if target_col.startswith("_"):
                continue

            if source_expr.startswith("FK:"):
                new_row[target_col] = resolve_bridge_fk(transformer, source_expr, row)
            elif source_expr.startswith("LITERAL:"):
                new_row[target_col] = source_expr[len("LITERAL:") :]
            else:
                new_row[target_col] = transformer._evaluate_expression(source_expr, row)

        # Only include rows where FKs resolved
        if all(v is not None for k, v in new_row.items() if k.endswith("_key")):
            transformed.append(new_row)

    logger.info(f"Transformed {len(transformed)} rows for {table_name}")
    return transformed


def resolve_bridge_fk(transformer: Transformer, fk_expr: str, row: dict) -> int | None:
    """
    Resolve FK for bridge tables.

    Args:
        transformer: Transformer instance for dimension lookups
        fk_expr: FK expression like "FK:dim_table.lookup_col|source_col"
        row: Source row dict

    Returns:
        Resolved foreign key or None
    """
    parts = fk_expr[3:].split("|")
    dim_ref = parts[0]
    source_col = parts[1] if len(parts) > 1 else None

    dim_table, _lookup_col = dim_ref.split(".")
    lookup_val = row.get(source_col) if source_col else None

    if lookup_val is None:
        return None

    return transformer.lookup_dimension_key(dim_table, lookup_val)


def parse_trial_locations(text: str, country_name_cache: dict[str, int]) -> list[str]:
    """Parse country names from a trial locations text field.

    Splits on ``|`` and ``;`` delimiters, applies aliases, and attempts
    to extract country names from address-like strings.

    Args:
        text: Raw locations string (e.g. "India|Egypt" or
              "Mount Sinai Hospital, Toronto, Ontario, Canada").
        country_name_cache: Mapping of canonical country name -> key
              (from dim_geography_by_country_name).

    Returns:
        Deduplicated list of canonical country names found in ``country_name_cache``.
    """
    # Build case-insensitive reverse lookup
    lower_to_canonical: dict[str, str] = {name.lower(): name for name in country_name_cache}
    # Also index aliases (case-insensitive)
    alias_lower: dict[str, str | None] = {k.lower(): v for k, v in COUNTRY_ALIASES.items()}

    matched: list[str] = []
    seen: set[str] = set()

    for raw_segment in re.split(r"[|;]", text):
        stripped = raw_segment.strip()
        if not stripped:
            continue

        resolved = _lookup_country_name(stripped.lower(), lower_to_canonical, alias_lower)
        if resolved is None and "," in stripped:
            # Address-like: "City, State, Country" — try last part
            last_part = stripped.rsplit(",", 1)[-1].strip()
            resolved = _lookup_country_name(last_part.lower(), lower_to_canonical, alias_lower)
        if resolved is None:
            # Try text inside parentheses: "Hospital Name (Country)"
            paren_match = re.search(r"\(([^)]+)\)", stripped)
            if paren_match:
                inner = paren_match.group(1).strip()
                resolved = _lookup_country_name(inner.lower(), lower_to_canonical, alias_lower)

        if resolved and resolved not in seen:
            seen.add(resolved)
            matched.append(resolved)

    return matched


def _lookup_country_name(
    text_lower: str,
    lower_to_canonical: dict[str, str],
    alias_lower: dict[str, str | None],
) -> str | None:
    """Resolve a lowered string to a canonical country name via alias or direct match."""
    if text_lower in alias_lower:
        aliased = alias_lower[text_lower]
        if aliased is None:
            return None
        return lower_to_canonical.get(aliased.lower())
    return lower_to_canonical.get(text_lower)


def collect_trial_geography_rows(transformer: Transformer, source_def: dict) -> list[dict]:
    """Collect bridge rows mapping trials to countries from free-text locations."""
    source_table = source_def["table"]
    trial_col = source_def["trial_col"]
    country_col = source_def["country_col"]
    country_name_cache = transformer._dim_caches.get("dim_geography_by_country_name", {})

    rows: list[dict] = []
    for row in transformer.extractor.extract_table(source_table):
        trial_id = row.get(trial_col)
        locations_text = row.get(country_col)
        trial_key = transformer.lookup_dimension_key("fact_clinical_trial_event", trial_id)
        if trial_key is None or not locations_text:
            continue
        for country_name in parse_trial_locations(locations_text, country_name_cache):
            country_key = country_name_cache.get(country_name)
            if country_key is not None:
                rows.append({"trial_key": trial_key, "country_key": country_key})
    return rows


def collect_structured_trial_geography_rows(
    transformer: Transformer,
    source_def: dict,
) -> list[dict]:
    """Collect bridge rows from a structured trial→country junction table."""
    source_table = source_def["table"]
    trial_col = source_def["trial_col"]
    country_col = source_def["country_col"]

    rows: list[dict] = []
    for row in transformer.extractor.extract_table(source_table):
        trial_id = row.get(trial_col)
        country_ref = row.get(country_col)

        trial_key = transformer.lookup_dimension_key("fact_clinical_trial_event", trial_id)
        country_key = transformer.lookup_dimension_key("dim_geography", country_ref)

        if trial_key is not None and country_key is not None:
            rows.append({"trial_key": trial_key, "country_key": country_key})
    return rows


def _collect_candidate_geography_rows(transformer: Transformer, source_def: dict) -> list[dict]:
    """Collect candidate-level geography rows from a single union source."""
    candidate_col = source_def["candidate_col"]
    country_col = source_def["country_col"]
    location_scope = source_def["location_scope"]

    rows: list[dict] = []
    for row in transformer.extractor.extract_table(source_def["table"]):
        candidate_id = row.get(candidate_col)
        country_ref = row.get(country_col)

        candidate_key = transformer.lookup_dimension_key("dim_candidate_core", candidate_id)

        if source_def.get("optionset_lookup"):
            country_name = transformer.extractor.lookup_optionset(source_def["optionset_lookup"], country_ref)
            if country_name is None:
                continue
            country_key = transformer._dim_caches.get("dim_geography_by_country_name", {}).get(country_name)
        elif source_def.get("country_name_lookup"):
            country_key = transformer._dim_caches.get("dim_geography_by_country_name", {}).get(country_ref)
        else:
            country_key = transformer.lookup_dimension_key("dim_geography", country_ref)

        if candidate_key is not None and country_key is not None:
            rows.append({"candidate_key": candidate_key, "country_key": country_key, "location_scope": location_scope})
    return rows


def _deduplicate(rows: list[dict], key_cols: tuple[str, ...]) -> list[dict]:
    """Remove duplicate rows based on the given key columns."""
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for row in rows:
        key = tuple(row[c] for c in key_cols)
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def transform_union_bridge(transformer: Transformer, table_name: str, _config: dict, special: dict) -> list[dict]:
    """
    Transform bridge table from multiple source tables (UNION).

    Args:
        transformer: Transformer instance
        table_name: Target bridge table name
        _config: Table config (unused but part of signature)
        special: Special handling config with union_sources

    Returns:
        List of bridge rows
    """
    union_sources = special.get("union_sources", [])
    is_trial_bridge = special.get("trial_bridge", False)
    transformed: list[dict] = []

    for source_def in union_sources:
        if is_trial_bridge:
            collector = (
                collect_trial_geography_rows
                if source_def.get("parse_trial_locations")
                else collect_structured_trial_geography_rows
            )
            transformed.extend(collector(transformer, source_def))
        else:
            transformed.extend(_collect_candidate_geography_rows(transformer, source_def))

    key_cols = ("trial_key", "country_key") if is_trial_bridge else ("candidate_key", "country_key", "location_scope")
    transformed = _deduplicate(transformed, key_cols)

    logger.info(f"Transformed {len(transformed)} rows for {table_name}")
    return transformed


def transform_delimited_bridge(transformer: Transformer, table_name: str, config: dict, special: dict) -> list[dict]:
    """
    Transform bridge table from delimited field.

    Creates one bridge row per (entity, delimited_value) pair.

    Args:
        transformer: Transformer instance
        table_name: Target bridge table name
        config: Table config from schema map
        special: Special handling config with source_column, delimiter, etc.

    Returns:
        List of bridge rows
    """
    source_table = config["_source_table"]
    source_column = special["source_column"]
    delimiter = special["delimiter"]
    dim_table = special["dimension_table"]

    # Derive the FK column name from config (the non-candidate, non-meta column)
    fk_col = next(col for col, expr in config.items() if not col.startswith("_") and col != "candidate_key")

    transformed = []

    # Derive candidate source column from config FK expression
    candidate_fk_expr = config.get("candidate_key", "")
    candidate_source_col = candidate_fk_expr.split("|")[-1] if "|" in candidate_fk_expr else "candidateid"

    for row in transformer.extractor.extract_table(source_table):
        # Get the candidate key first
        candidate_id = row.get(candidate_source_col)
        candidate_key = transformer.lookup_dimension_key("dim_candidate_core", candidate_id)

        if candidate_key is None:
            continue

        # Parse the delimited field
        delimited_value = row.get(source_column)
        if not delimited_value:
            continue

        # Split and create bridge rows
        parts = [p.strip() for p in delimited_value.split(delimiter)]
        for part in parts:
            if not part:
                continue

            # Look up the dimension key
            dim_key = transformer.lookup_dimension_key(dim_table, part)

            if dim_key is not None:
                transformed.append({
                    "candidate_key": candidate_key,
                    fk_col: dim_key,
                })

    logger.info(f"Transformed {len(transformed)} rows for {table_name}")
    return transformed
