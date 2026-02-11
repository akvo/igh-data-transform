"""
Loader module for writing transformed data to target database.

Handles:
- Creating target database schema
- Inserting transformed data
- Maintaining referential integrity through load order
"""

import logging
import sqlite3
from pathlib import Path

from config.schema_map import STAR_SCHEMA_MAP
from src.ddl_generator import generate_all_ddl

logger = logging.getLogger(__name__)


class Loader:
    """Loads transformed data into target star schema database."""

    def __init__(self, db_path: str | Path):
        """
        Initialize loader with target database path.

        Args:
            db_path: Path to target database (will be created/overwritten)
        """
        self.db_path = Path(db_path)
        self._conn = None

    def connect(self) -> None:
        """Create/overwrite and connect to target database."""
        # Remove existing database for idempotent runs
        if self.db_path.exists():
            self.db_path.unlink()
            logger.info(f"Removed existing database: {self.db_path}")

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        logger.info(f"Created target database: {self.db_path}")

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Closed target database connection")

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

    def create_schema(self) -> None:
        """Create all tables in the target database."""
        cursor = self._get_cursor()
        ddl_statements = generate_all_ddl()

        for ddl in ddl_statements:
            logger.debug(f"Executing DDL: {ddl[:100]}...")
            cursor.execute(ddl)

        self._conn.commit()
        logger.info(f"Created {len(ddl_statements)} tables")

    def load_table(self, table_name: str, data: list[dict]) -> list[dict]:
        """
        Load data into a table and return with generated keys.

        Args:
            table_name: Target table name
            data: List of dicts with column->value mappings

        Returns:
            List of dicts with primary key values added
        """
        if not data:
            logger.info(f"No data to load for {table_name}")
            return []

        cursor = self._get_cursor()
        config = STAR_SCHEMA_MAP.get(table_name, {})
        pk_col = config.get("_pk")

        # Get columns from first row (excluding PK which is auto-generated)
        columns = [col for col in data[0] if col != pk_col]
        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(columns)

        # S608: table_name and columns from STAR_SCHEMA_MAP config, not user input
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"  # noqa: S608

        # Insert rows and capture generated keys
        result_data = []
        for row in data:
            values = [row.get(col) for col in columns]
            cursor.execute(insert_sql, values)

            # Add the auto-generated PK to the row
            if pk_col:
                row_with_pk = dict(row)
                row_with_pk[pk_col] = cursor.lastrowid
                result_data.append(row_with_pk)
            else:
                result_data.append(row)

        self._conn.commit()
        logger.info(f"Loaded {len(data)} rows into {table_name}")

        return result_data

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        cursor = self._get_cursor()
        # S608: table_name from STAR_SCHEMA_MAP config, not user input
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
        return cursor.fetchone()[0]

    def verify_foreign_keys(self) -> dict[str, list[str]]:
        """
        Verify FK integrity across all tables.

        Returns:
            Dict of {table: [error messages]} for any violations
        """
        cursor = self._get_cursor()
        issues: dict[str, list[str]] = {}

        fk_checks = [
            # fact_pipeline_snapshot
            ("fact_pipeline_snapshot", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("fact_pipeline_snapshot", "product_key", "dim_product", "product_key"),
            ("fact_pipeline_snapshot", "disease_key", "dim_disease", "disease_key"),
            ("fact_pipeline_snapshot", "secondary_disease_key", "dim_disease", "disease_key"),
            ("fact_pipeline_snapshot", "sub_product_key", "dim_product", "product_key"),
            ("fact_pipeline_snapshot", "phase_key", "dim_phase", "phase_key"),
            # fact_clinical_trial_event
            ("fact_clinical_trial_event", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("fact_clinical_trial_event", "disease_key", "dim_disease", "disease_key"),
            ("fact_clinical_trial_event", "product_key", "dim_product", "product_key"),
            # fact_publication
            ("fact_publication", "candidate_key", "dim_candidate_core", "candidate_key"),
            # bridge_candidate_geography
            ("bridge_candidate_geography", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_geography", "country_key", "dim_geography", "country_key"),
            # bridge_candidate_developer
            ("bridge_candidate_developer", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_developer", "developer_key", "dim_developer", "developer_key"),
            # dim_priority
            ("dim_priority", "disease_key", "dim_disease", "disease_key"),
            # bridge_candidate_priority
            ("bridge_candidate_priority", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_priority", "priority_key", "dim_priority", "priority_key"),
            # bridge_candidate_age_group
            ("bridge_candidate_age_group", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_age_group", "age_group_key", "dim_age_group", "age_group_key"),
            # bridge_candidate_approving_authority
            ("bridge_candidate_approving_authority", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_approving_authority", "authority_key", "dim_approving_authority", "authority_key"),
            # bridge_candidate_organization
            ("bridge_candidate_organization", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_organization", "organization_key", "dim_organization", "organization_key"),
            # bridge_candidate_funder
            ("bridge_candidate_funder", "candidate_key", "dim_candidate_core", "candidate_key"),
            ("bridge_candidate_funder", "funder_key", "dim_funder", "funder_key"),
            # bridge_trial_geography
            ("bridge_trial_geography", "trial_key", "fact_clinical_trial_event", "trial_id"),
            ("bridge_trial_geography", "country_key", "dim_geography", "country_key"),
        ]

        for fk_check in fk_checks:
            self._check_single_fk(cursor, fk_check, issues)

        return issues

    @staticmethod
    def _check_single_fk(
        cursor: sqlite3.Cursor,
        fk_check: tuple[str, str, str, str],
        issues: dict[str, list[str]],
    ) -> None:
        """Check a single FK relationship for orphans."""
        fact_table, fk_col, dim_table, pk_col = fk_check
        try:
            # Find orphan FKs (non-null FKs that don't exist in dimension)
            # S608: table/column names are hardcoded in verify_foreign_keys, not user input
            sql = f"""
                SELECT COUNT(*) FROM {fact_table} f
                WHERE f.{fk_col} IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM {dim_table} d
                    WHERE d.{pk_col} = f.{fk_col}
                )
            """  # noqa: S608
            cursor.execute(sql)
            orphan_count = cursor.fetchone()[0]

            if orphan_count > 0:
                if fact_table not in issues:
                    issues[fact_table] = []
                issues[fact_table].append(f"{orphan_count} orphan {fk_col} values (missing in {dim_table})")
        except sqlite3.Error as e:
            logger.warning(f"Could not verify FK {fact_table}.{fk_col}: {e}")

    def print_summary(self) -> None:
        """Print summary of loaded tables."""
        cursor = self._get_cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        print("\n" + "=" * 50)
        print("Target Database Summary")
        print("=" * 50)

        for table in tables:
            count = self.get_row_count(table)
            print(f"  {table}: {count:,} rows")

        print("=" * 50)

        # Check FK integrity
        issues = self.verify_foreign_keys()
        if issues:
            print("\nFK Integrity Issues:")
            for table, errors in issues.items():
                for error in errors:
                    print(f"  {table}: {error}")
        else:
            print("\nFK Integrity: OK")
