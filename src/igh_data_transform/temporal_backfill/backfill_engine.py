"""Main orchestrator for temporal backfill."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .schema_transformer import SchemaTransformer
from .scd2_generator import SCD2Generator
from .temporal_analyzer import TemporalAnalyzer


class BackfillEngine:
    """Orchestrates the temporal backfill process."""

    def __init__(
        self,
        raw_db: str,
        report_path: str,
        output_db: str,
        current_year: int | None = None,
    ):
        """
        Initialize backfill engine.

        Args:
            raw_db: Path to raw database
            report_path: Path to temporal_columns_report.json
            output_db: Path to output database
            current_year: Year to use for current-year columns (no year suffix)
        """
        self.raw_db = raw_db
        self.report_path = report_path
        self.output_db = output_db
        self.current_year = current_year

        # Load temporal report
        with open(report_path, encoding="utf-8") as f:
            self.report = json.load(f)

        self.stats = {
            "tables_processed": 0,
            "entities_processed": 0,
            "versions_created": 0,
            "temporal_columns_removed": 0,
            "base_columns_created": 0,
        }

    def run(self, dry_run: bool = False):
        """
        Run the backfill process.

        Args:
            dry_run: If True, show what would be done without creating output
        """
        print("=" * 80)
        print("SCD2 TEMPORAL BACKFILL")
        print("=" * 80)
        print()

        # Phase 1: Load report
        print("[1/5] Loading temporal column report...")
        tables_with_temporal = self.report.get("tables", {})
        total_groups = self.report["summary"]["total_temporal_groups"]
        total_temporal_cols = self.report["summary"]["total_temporal_columns"]

        print(f"  ✓ Found {len(tables_with_temporal)} table(s) with temporal columns")
        print(f"  ✓ Found {total_groups} temporal groups")
        print(f"  ✓ Total temporal columns: {total_temporal_cols}")
        print()

        if dry_run:
            print("DRY RUN - No output will be created")
            print()
            self._show_dry_run_summary(tables_with_temporal)
            return

        # Phase 2: Create output database
        print("[2/5] Creating output database...")
        output_path = Path(self.output_db)
        if output_path.exists():
            output_path.unlink()
            print(f"  ✓ Deleted existing: {self.output_db}")

        # Create new database
        output_conn = sqlite3.connect(self.output_db)
        output_conn.row_factory = sqlite3.Row
        print(f"  ✓ Created: {self.output_db}")
        print()

        # Phase 3: Copy non-temporal tables
        print("[3/5] Copying non-temporal tables...")
        self._copy_non_temporal_tables(output_conn, tables_with_temporal)
        print()

        # Phase 4: Backfill temporal tables
        print("[4/5] Backfilling temporal columns...")
        for table_name, table_data in tables_with_temporal.items():
            self._backfill_table(output_conn, table_name, table_data)
        print()

        # Phase 5: Create indexes and optimize
        print("[5/5] Creating indexes and optimizing...")
        for table_name in tables_with_temporal.keys():
            self._create_indexes(output_conn, table_name)
        output_conn.execute("VACUUM")
        print("  ✓ Vacuum complete")
        print()

        output_conn.close()

        # Print summary
        print("=" * 80)
        print("✓ BACKFILL COMPLETE")
        print("=" * 80)
        print(f"Output database: {self.output_db}")
        print()
        print("Summary:")
        print(f"  Tables processed: {self.stats['tables_processed']}")
        print(f"  Temporal groups: {total_groups}")
        print(f"  Entities processed: {self.stats['entities_processed']}")
        print(f"  SCD2 versions created: {self.stats['versions_created']}")
        print(f"  Original temporal columns removed: {self.stats['temporal_columns_removed']}")
        print(f"  Base columns created: {self.stats['base_columns_created']}")
        print()

    def _show_dry_run_summary(self, tables_with_temporal: dict[str, Any]):
        """Show what would be done in dry run mode."""
        for table_name, table_data in tables_with_temporal.items():
            temporal_groups = table_data["temporal_groups"]
            num_temporal_cols = table_data["temporal_columns_count"]

            print(f"\nTable: {table_name}")
            print(f"  Total columns: {table_data['total_columns']}")
            print(f"  Temporal columns to remove: {num_temporal_cols}")
            print(f"  Base columns to create: {len(temporal_groups)}")
            print(f"  Temporal groups:")

            for base_name, columns in temporal_groups.items():
                years = [str(col["year"]) for col in columns]
                print(f"    {base_name}: {', '.join(years)}")

    def _copy_non_temporal_tables(
        self, output_conn: sqlite3.Connection, tables_with_temporal: dict[str, Any]
    ):
        """Copy all tables that don't have temporal columns."""
        raw_conn = sqlite3.connect(self.raw_db)
        raw_cursor = raw_conn.cursor()

        # Get all table names
        raw_cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        all_tables = [row[0] for row in raw_cursor.fetchall()]

        temporal_table_names = set(tables_with_temporal.keys())

        for table_name in all_tables:
            if table_name in temporal_table_names:
                continue  # Will be handled in backfill phase

            # Get table schema
            raw_cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            create_sql = raw_cursor.fetchone()[0]

            # Create table in output
            output_conn.execute(create_sql)

            # Copy data
            raw_cursor.execute(f"SELECT * FROM {table_name}")  # noqa: S608
            rows = raw_cursor.fetchall()

            if rows:
                # Get column names
                column_names = [desc[0] for desc in raw_cursor.description]
                placeholders = ", ".join(["?"] * len(column_names))
                columns_str = ", ".join(column_names)

                # S608: table_name from schema, not user input
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"  # noqa: S608

                output_conn.executemany(insert_sql, rows)

            print(f"  ✓ Copied {table_name} ({len(rows)} rows)")

        output_conn.commit()
        raw_conn.close()

    def _backfill_table(
        self,
        output_conn: sqlite3.Connection,
        table_name: str,
        table_data: dict[str, Any],
    ):
        """Backfill a table with temporal columns."""
        print(f"\nProcessing {table_name}...")

        temporal_groups = table_data["temporal_groups"]
        total_cols = table_data["total_columns"]
        num_temporal_cols = table_data["temporal_columns_count"]
        num_base_cols = len(temporal_groups)

        print(
            f"  Transforming schema ({total_cols} → {total_cols - num_temporal_cols + num_base_cols} columns)..."
        )

        # Get original schema
        raw_conn = sqlite3.connect(self.raw_db)
        raw_cursor = raw_conn.cursor()
        raw_cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        original_schema = raw_cursor.fetchone()[0]

        # Transform schema
        transformer = SchemaTransformer(temporal_groups)
        new_schema = transformer.transform_schema(original_schema)

        # Create table in output
        output_conn.execute(new_schema)
        print("  ✓ Schema created")
        print()

        # Identify business key column
        business_key_column = self._identify_business_key(raw_conn, table_name)
        print(f"  Using business key: {business_key_column}")

        # Initialize analyzer and generator
        analyzer = TemporalAnalyzer(raw_conn, table_name, temporal_groups)
        all_base_columns = set(temporal_groups.keys())
        generator = SCD2Generator(
            current_year=self.current_year,
            all_base_columns=all_base_columns,
        )

        # Get all entity IDs
        entity_ids = analyzer.get_all_entity_ids(business_key_column)
        print(f"  Processing {len(entity_ids)} unique entities...")
        print()

        # Process each entity
        versions_for_table = 0
        show_progress_every = 100

        for idx, entity_id in enumerate(entity_ids, 1):
            # Get temporal data
            temporal_data = analyzer.analyze_entity(entity_id, business_key_column)

            # Get base record
            base_record = analyzer.get_base_record(entity_id, business_key_column)

            # Generate SCD2 versions
            versions = generator.generate_versions(
                entity_id, business_key_column, base_record, temporal_data
            )

            # Insert versions
            self._insert_versions(output_conn, table_name, versions)

            versions_for_table += len(versions)

            # Show progress
            if idx == 1 or idx % show_progress_every == 0 or idx == len(entity_ids):
                summary = generator.format_version_summary(versions)
                print(
                    f"  Entity {idx}/{len(entity_ids)} ({business_key_column}: {entity_id}): {summary}"
                )

        output_conn.commit()

        print()
        print(f"  ✓ Processed {len(entity_ids)} entities")
        if entity_ids:
            avg_versions = versions_for_table / len(entity_ids)
            print(f"  ✓ Created {versions_for_table} SCD2 versions (avg {avg_versions:.1f} per entity)")
        else:
            print(f"  ✓ Created {versions_for_table} SCD2 versions")

        # Update stats
        self.stats["tables_processed"] += 1
        self.stats["entities_processed"] += len(entity_ids)
        self.stats["versions_created"] += versions_for_table
        self.stats["temporal_columns_removed"] += num_temporal_cols
        self.stats["base_columns_created"] += num_base_cols

        raw_conn.close()

    def _identify_business_key(
        self, conn: sqlite3.Connection, table_name: str
    ) -> str:
        """
        Identify the business key column for a table.

        Args:
            conn: Database connection
            table_name: Table name

        Returns:
            Business key column name
        """
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")  # noqa: S608

        # Look for primary key that's not row_id
        for row in cursor.fetchall():
            col_name = row[1]
            is_pk = row[5]

            if col_name != "row_id" and is_pk:
                return col_name

        # If no PK found (besides row_id), look for common patterns
        cursor.execute(f"PRAGMA table_info({table_name})")  # noqa: S608
        for row in cursor.fetchall():
            col_name = row[1]
            if col_name.endswith("_id") and col_name != "row_id":
                return col_name

        # Fallback: use row_id
        return "row_id"

    def _insert_versions(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        versions: list[dict[str, Any]],
    ):
        """Insert SCD2 versions into the table."""
        if not versions:
            return

        # Get column names from first version
        columns = list(versions[0].keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

        # S608: table_name from schema, not user input
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"  # noqa: S608

        # Convert dicts to tuples
        rows = [tuple(v[col] for col in columns) for v in versions]

        conn.executemany(insert_sql, rows)

    def _create_indexes(self, conn: sqlite3.Connection, table_name: str):
        """Create indexes for efficient SCD2 queries."""
        # Get business key column
        raw_conn = sqlite3.connect(self.raw_db)
        business_key = self._identify_business_key(raw_conn, table_name)
        raw_conn.close()

        # Create composite index (business_key, valid_to)
        index_name = f"idx_{table_name}_{business_key}_valid_to"
        # S608: table_name and business_key from schema, not user input
        index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({business_key}, valid_to)"  # noqa: S608

        conn.execute(index_sql)
        print(f"  ✓ Created index: {index_name}")

        conn.commit()
