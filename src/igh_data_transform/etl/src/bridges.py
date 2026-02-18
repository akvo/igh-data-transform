"""
Bridge transformation functions for transformer.

Handles transformation of bridge tables including:
- Standard bridges
- Union bridges (multiple source tables)
- Delimited bridges (from delimited fields)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from config.schema_map import STAR_SCHEMA_MAP

if TYPE_CHECKING:
    from src.transformer import Transformer

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
    transformed = []

    for source_def in union_sources:
        source_table = source_def["table"]
        candidate_col = source_def["candidate_col"]
        country_col = source_def["country_col"]
        location_scope = source_def["location_scope"]

        for row in transformer.extractor.extract_table(source_table):
            candidate_id = row.get(candidate_col)
            country_ref = row.get(country_col)

            # Resolve candidate key
            candidate_key = transformer.lookup_dimension_key("dim_candidate_core", candidate_id)

            # Resolve country key (might need optionset lookup for trial locations)
            if source_def.get("optionset_lookup"):
                # Option code -> country name via optionset
                optionset_name = source_def["optionset_lookup"]
                country_name = transformer.extractor.lookup_optionset(optionset_name, country_ref)
                if country_name is None:
                    continue
                # Country name -> country_key via secondary cache
                country_key = transformer._dim_caches.get("dim_geography_by_country_name", {}).get(country_name)
            else:
                country_key = transformer.lookup_dimension_key("dim_geography", country_ref)

            if candidate_key is not None and country_key is not None:
                transformed.append({
                    "candidate_key": candidate_key,
                    "country_key": country_key,
                    "location_scope": location_scope,
                })

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
