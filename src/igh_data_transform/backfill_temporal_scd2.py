#!/usr/bin/env python3
"""
SCD2 Temporal Backfill Tool

Backfills temporal columns (with year indicators) into proper SCD2 format
with single base columns and temporal versioning via valid_from/valid_to.
"""

import sys

import click

from lib.temporal_backfill import BackfillEngine


@click.command()
@click.option(
    "--raw-db",
    "-r",
    required=True,
    type=click.Path(exists=True),
    help="Path to raw SQLite database file",
)
@click.option(
    "--column-report",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to temporal_columns_report.json",
)
@click.option(
    "--output",
    "-o",
    default="scd2_backfilled.db",
    type=click.Path(),
    help="Output database path",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without creating output",
)
@click.option(
    "--current-year",
    type=int,
    default=2025,
    help="Year to use for current-year columns (columns without year suffix). Default: 2025",
)
def main(raw_db, column_report, output, dry_run, current_year):
    """
    Backfill temporal columns into SCD2 format.

    This tool identifies columns with year indicators (e.g., column_2021, column_2023)
    and consolidates them into a single base column with proper SCD2 versioning.

    Example:
        ./backfill_temporal_scd2.py \\
          --raw-db dataverse_complete.db \\
          --column-report temporal_columns_report.json \\
          --output scd2_backfilled.db
    """
    try:
        engine = BackfillEngine(raw_db, column_report, output, current_year=current_year)
        engine.run(dry_run=dry_run)
    except Exception as e:
        click.echo(f"\n✗ Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
