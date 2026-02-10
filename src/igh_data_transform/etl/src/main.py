"""
Main ETL entry point.

Orchestrates the extract-transform-load process to convert
Dataverse raw schema to OLAP star schema.

Usage:
    python -m src.main --source ../dataverse_complete.db --output output/star_schema.db
"""

import argparse
import logging
import sys
from pathlib import Path

from config.schema_map import STAR_SCHEMA_MAP, TABLE_LOAD_ORDER
from src.extractor import Extractor
from src.loader import Loader
from src.transformer import Transformer

# Map of dimension tables to their natural key columns
# Note: dim_phase uses phase_name for lookups (to match extracted phase from "Phase I - Drugs")
DIMENSION_NATURAL_KEYS = {
    "dim_product": "vin_productid",
    "dim_candidate_core": "vin_candidateid",
    "dim_disease": "vin_diseaseid",
    "dim_phase": "phase_name",  # Use phase_name for FK lookups
    "dim_geography": "vin_countryid",
    "dim_organization": "accountid",
    "dim_priority": "vin_rdpriorityid",
    "dim_date": "full_date",
    "dim_developer": "developer_name",  # Extracted from delimited vin_developersaggregated
    "dim_age_group": "option_code",
    "dim_approving_authority": "option_code",
    "dim_funder": "funder_name",
}

# Fact tables that need their keys cached for bridge FK resolution
FACT_NATURAL_KEYS = {
    "fact_clinical_trial_event": "vin_clinicaltrialid",
}


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _transform_table(transformer: Transformer, table_name: str, logger: logging.Logger) -> list[dict] | None:
    """Transform a single table based on its type."""
    if table_name.startswith("dim_"):
        return transformer.transform_dimension(table_name)
    if table_name.startswith("fact_"):
        return transformer.transform_fact(table_name)
    if table_name.startswith("bridge_"):
        return transformer.transform_bridge(table_name)

    logger.warning(f"Unknown table type: {table_name}")
    return None


def _cache_dimension_keys(
    transformer: Transformer,
    table_name: str,
    loaded_data: list[dict],
    config: dict,
) -> None:
    """Cache dimension keys for FK lookups."""
    pk_col = config.get("_pk")
    special = config.get("_special", {})

    if special.get("distinct"):
        # Composite key cache
        key_cols = special.get("distinct_cols", [])
        transformer.cache_composite_keys(table_name, loaded_data, pk_col, key_cols)
    else:
        # Find the natural key column (usually the ID column)
        lookup_col = DIMENSION_NATURAL_KEYS.get(table_name)
        if lookup_col:
            transformer.cache_dimension_keys(table_name, loaded_data, pk_col, lookup_col)

    # Add secondary cache for geography by country_name (for optionset lookups)
    if table_name == "dim_geography":
        transformer.cache_dimension_keys_by_name(table_name, loaded_data, pk_col, "country_name")


def _process_tables(transformer: Transformer, loader: Loader, logger: logging.Logger) -> None:
    """Process all tables in dependency order."""
    for table_name in TABLE_LOAD_ORDER:
        logger.info(f"Processing {table_name}...")

        config = STAR_SCHEMA_MAP.get(table_name)
        if not config:
            logger.warning(f"No config for {table_name}, skipping")
            continue

        # Transform table
        data = _transform_table(transformer, table_name, logger)
        if data is None:
            continue

        # Load and get back data with generated keys
        loaded_data = loader.load_table(table_name, data)

        # Cache dimension keys for FK lookups
        if table_name.startswith("dim_"):
            _cache_dimension_keys(transformer, table_name, loaded_data, config)

        # Cache fact keys needed by bridges (e.g. trial_id by vin_clinicaltrialid)
        if table_name in FACT_NATURAL_KEYS:
            pk_col = config.get("_pk")
            lookup_col = FACT_NATURAL_KEYS[table_name]
            transformer.cache_dimension_keys(table_name, loaded_data, pk_col, lookup_col)

        # Build candidate cross-reference maps after pipeline snapshot is loaded
        if table_name == "fact_pipeline_snapshot":
            transformer.build_candidate_cross_refs(loaded_data)


def run_etl(source_path: Path, output_path: Path) -> bool:
    """
    Run the full ETL pipeline.

    Args:
        source_path: Path to source Dataverse database
        output_path: Path to output star schema database

    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting ETL: {source_path} -> {output_path}")

    try:
        with Extractor(source_path) as extractor:
            extractor.build_optionset_cache()

            with Loader(output_path) as loader:
                loader.create_schema()
                transformer = Transformer(extractor)
                _process_tables(transformer, loader, logger)
                loader.print_summary()

    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        return False
    else:
        logger.info("ETL completed successfully")
        return True


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Transform Dataverse raw schema to OLAP star schema")
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="Path to source Dataverse database",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to output star schema database",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    success = run_etl(args.source, args.output)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
