"""End-to-end tests for the Silver-to-Gold star schema transformation.

These tests exercise the full pipeline against a real Silver database
derived from a Bronze database synced from the Dataverse API.

Run with:  uv run pytest --e2e -v -k silver_to_gold
"""

import sqlite3

import pandas as pd
import pytest

pytestmark = pytest.mark.e2e

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_table(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM {table}", conn)  # noqa: S608


def _table_names(conn: sqlite3.Connection) -> set[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {row[0] for row in cur.fetchall()}


def _row_count(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.execute(f"SELECT COUNT(*) FROM [{table}]")  # noqa: S608
    return cur.fetchone()[0]


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info([{table}])")  # noqa: S608
    return {row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Expected table sets (from schema_map.TABLE_LOAD_ORDER + _etl_metadata)
# ---------------------------------------------------------------------------

DIMENSION_TABLES = {
    "dim_product",
    "dim_disease",
    "dim_phase",
    "dim_geography",
    "dim_organization",
    "dim_priority",
    "dim_date",
    "dim_age_group",
    "dim_approving_authority",
    "dim_candidate_core",
    "dim_candidate_tech",
    "dim_candidate_regulatory",
    "dim_developer",
    "dim_funder",
}

FACT_TABLES = {
    "fact_pipeline_snapshot",
    "fact_clinical_trial_event",
    "fact_publication",
}

BRIDGE_TABLES = {
    "bridge_candidate_geography",
    "bridge_candidate_developer",
    "bridge_candidate_priority",
    "bridge_candidate_age_group",
    "bridge_candidate_approving_authority",
    "bridge_candidate_organization",
    "bridge_candidate_funder",
    "bridge_trial_geography",
}

ALL_STAR_TABLES = DIMENSION_TABLES | FACT_TABLES | BRIDGE_TABLES | {"_etl_metadata"}


# ---------------------------------------------------------------------------
# 1. Completeness
# ---------------------------------------------------------------------------


class TestGoldCompleteness:
    """Verify the Gold DB was created and contains expected tables."""

    def test_gold_db_exists(self, gold_db_path):
        assert gold_db_path.exists()

    def test_all_star_tables_present(self, gold_conn):
        tables = _table_names(gold_conn)
        missing = ALL_STAR_TABLES - tables
        assert not missing, f"Missing tables: {missing}"

    def test_no_unexpected_tables(self, gold_conn):
        """Only expected star schema tables should be in the Gold DB."""
        tables = _table_names(gold_conn)
        unexpected = tables - ALL_STAR_TABLES
        assert not unexpected, f"Unexpected tables: {unexpected}"

    def test_etl_metadata_populated(self, gold_conn):
        count = _row_count(gold_conn, "_etl_metadata")
        assert count > 0, "_etl_metadata is empty"


# ---------------------------------------------------------------------------
# 2. Dimensions
# ---------------------------------------------------------------------------


class TestDimensionTables:
    """Verify dimension tables are populated and well-formed."""

    @pytest.mark.parametrize("table", sorted(DIMENSION_TABLES))
    def test_dimension_has_rows(self, gold_conn, table):
        count = _row_count(gold_conn, table)
        assert count > 0, f"{table} is empty"

    # -- dim_date --

    def test_dim_date_spans_expected_years(self, gold_conn):
        """dim_date should cover 2015-2030 per schema_map config."""
        df = _read_table(gold_conn, "dim_date")
        years = set(df["year"].unique())
        expected = set(range(2015, 2031))
        missing = expected - years
        assert not missing, f"dim_date missing years: {missing}"

    def test_dim_date_has_quarter(self, gold_conn):
        df = _read_table(gold_conn, "dim_date")
        assert "quarter" in df.columns
        quarters = set(df["quarter"].unique())
        assert quarters == {1, 2, 3, 4}

    # -- dim_phase --

    def test_dim_phase_has_sort_order(self, gold_conn):
        cols = _column_names(gold_conn, "dim_phase")
        assert "sort_order" in cols

    def test_dim_phase_sort_order_is_populated(self, gold_conn):
        df = _read_table(gold_conn, "dim_phase")
        assert df["sort_order"].notna().any(), "sort_order is all NULL"

    # -- dim_candidate_core --

    def test_dim_candidate_core_no_null_candidateid(self, gold_conn):
        df = _read_table(gold_conn, "dim_candidate_core")
        nulls = df["candidateid"].isna().sum()
        assert nulls == 0, f"{nulls} null candidateid values"

    def test_dim_candidate_core_has_expected_columns(self, gold_conn):
        cols = _column_names(gold_conn, "dim_candidate_core")
        expected = {"candidate_key", "candidateid", "candidate_name", "developers_agg"}
        missing = expected - cols
        assert not missing, f"Missing columns: {missing}"

    # -- dim_disease --

    def test_dim_disease_no_null_diseaseid(self, gold_conn):
        df = _read_table(gold_conn, "dim_disease")
        nulls = df["diseaseid"].isna().sum()
        assert nulls == 0, f"{nulls} null diseaseid values"

    def test_dim_disease_has_expected_columns(self, gold_conn):
        cols = _column_names(gold_conn, "dim_disease")
        expected = {"disease_key", "diseaseid", "disease_name", "disease_group_name", "global_health_area"}
        missing = expected - cols
        assert not missing, f"Missing columns: {missing}"

    # -- dim_geography --

    def test_dim_geography_has_iso_code(self, gold_conn):
        cols = _column_names(gold_conn, "dim_geography")
        assert "iso_code" in cols

    # -- dim_candidate_tech --

    def test_dim_candidate_tech_has_technology_type(self, gold_conn):
        df = _read_table(gold_conn, "dim_candidate_tech")
        assert "technology_type" in df.columns
        assert df["technology_type"].notna().all(), "technology_type has NULLs (should COALESCE to 'Unknown')"


# ---------------------------------------------------------------------------
# 3. Fact tables
# ---------------------------------------------------------------------------


class TestFactTables:
    """Verify fact tables are populated with valid FK references."""

    # -- fact_pipeline_snapshot --

    def test_pipeline_snapshot_has_rows(self, gold_conn):
        count = _row_count(gold_conn, "fact_pipeline_snapshot")
        assert count > 0, "fact_pipeline_snapshot is empty"

    def test_pipeline_snapshot_has_fk_columns(self, gold_conn):
        cols = _column_names(gold_conn, "fact_pipeline_snapshot")
        expected_fks = {"candidate_key", "product_key", "disease_key", "phase_key", "date_key"}
        missing = expected_fks - cols
        assert not missing, f"Missing FK columns: {missing}"

    def test_pipeline_snapshot_candidate_key_not_all_null(self, gold_conn):
        df = _read_table(gold_conn, "fact_pipeline_snapshot")
        assert df["candidate_key"].notna().any(), "candidate_key is all NULL"

    # -- fact_clinical_trial_event --

    def test_clinical_trial_has_rows(self, gold_conn):
        count = _row_count(gold_conn, "fact_clinical_trial_event")
        assert count > 0, "fact_clinical_trial_event is empty"

    def test_clinical_trial_has_fk_columns(self, gold_conn):
        cols = _column_names(gold_conn, "fact_clinical_trial_event")
        expected_fks = {"candidate_key", "disease_key", "product_key", "start_date_key"}
        missing = expected_fks - cols
        assert not missing, f"Missing FK columns: {missing}"

    # -- fact_publication --

    def test_publication_has_expected_columns(self, gold_conn):
        cols = _column_names(gold_conn, "fact_publication")
        expected = {"publication_id", "candidate_key", "title", "url"}
        missing = expected - cols
        assert not missing, f"Missing columns: {missing}"


# ---------------------------------------------------------------------------
# 4. Bridge tables
# ---------------------------------------------------------------------------


class TestBridgeTables:
    """Verify bridge tables link dimensions to facts correctly."""

    # Bridges whose source junction tables may have too few rows to survive
    # FK resolution against the dimension tables.
    _SPARSE_BRIDGES = {"bridge_candidate_organization"}

    @pytest.mark.parametrize("table", sorted(BRIDGE_TABLES))
    def test_bridge_has_rows(self, gold_conn, table):
        count = _row_count(gold_conn, table)
        if table in self._SPARSE_BRIDGES:
            # Source data may be too sparse for FK resolution to produce rows
            pytest.skip(f"{table} may be empty due to sparse source data") if count == 0 else None
        else:
            assert count > 0, f"{table} is empty"

    def test_bridge_candidate_developer_columns(self, gold_conn):
        cols = _column_names(gold_conn, "bridge_candidate_developer")
        assert {"candidate_key", "developer_key"}.issubset(cols)

    def test_bridge_candidate_geography_has_scope(self, gold_conn):
        cols = _column_names(gold_conn, "bridge_candidate_geography")
        assert "location_scope" in cols

    def test_bridge_trial_geography_columns(self, gold_conn):
        cols = _column_names(gold_conn, "bridge_trial_geography")
        assert {"trial_key", "country_key"}.issubset(cols)


# ---------------------------------------------------------------------------
# 5. Referential Integrity
# ---------------------------------------------------------------------------


class TestReferentialIntegrity:
    """Verify FK columns reference valid dimension keys."""

    # -- fact_pipeline_snapshot FKs --

    def test_snapshot_candidate_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_candidate_core")
        refs = set(snap["candidate_key"].dropna().unique())
        valid = set(dim["candidate_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan candidate_key values in fact_pipeline_snapshot"

    def test_snapshot_disease_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_disease")
        refs = set(snap["disease_key"].dropna().unique())
        valid = set(dim["disease_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan disease_key values in fact_pipeline_snapshot"

    def test_snapshot_phase_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_phase")
        refs = set(snap["phase_key"].dropna().unique())
        valid = set(dim["phase_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan phase_key values in fact_pipeline_snapshot"

    def test_snapshot_date_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_date")
        refs = set(snap["date_key"].dropna().unique())
        valid = set(dim["date_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan date_key values in fact_pipeline_snapshot"

    def test_snapshot_product_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_product")
        refs = set(snap["product_key"].dropna().unique())
        valid = set(dim["product_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan product_key values in fact_pipeline_snapshot"

    def test_snapshot_technology_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_candidate_tech")
        refs = set(snap["technology_key"].dropna().unique())
        valid = set(dim["technology_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan technology_key values in fact_pipeline_snapshot"

    def test_snapshot_regulatory_key_valid(self, gold_conn):
        snap = _read_table(gold_conn, "fact_pipeline_snapshot")
        dim = _read_table(gold_conn, "dim_candidate_regulatory")
        refs = set(snap["regulatory_key"].dropna().unique())
        valid = set(dim["regulatory_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan regulatory_key values in fact_pipeline_snapshot"

    # -- fact_clinical_trial_event FKs --

    def test_trial_candidate_key_valid(self, gold_conn):
        trial = _read_table(gold_conn, "fact_clinical_trial_event")
        dim = _read_table(gold_conn, "dim_candidate_core")
        refs = set(trial["candidate_key"].dropna().unique())
        valid = set(dim["candidate_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan candidate_key values in fact_clinical_trial_event"

    def test_trial_disease_key_valid(self, gold_conn):
        trial = _read_table(gold_conn, "fact_clinical_trial_event")
        dim = _read_table(gold_conn, "dim_disease")
        refs = set(trial["disease_key"].dropna().unique())
        valid = set(dim["disease_key"].unique())
        orphans = refs - valid
        assert not orphans, f"{len(orphans)} orphan disease_key values in fact_clinical_trial_event"

    # -- Bridge FKs --

    def test_bridge_candidate_developer_fks_valid(self, gold_conn):
        bridge = _read_table(gold_conn, "bridge_candidate_developer")
        cand = _read_table(gold_conn, "dim_candidate_core")
        dev = _read_table(gold_conn, "dim_developer")

        orphan_cand = set(bridge["candidate_key"].dropna().unique()) - set(cand["candidate_key"].unique())
        orphan_dev = set(bridge["developer_key"].dropna().unique()) - set(dev["developer_key"].unique())
        assert not orphan_cand, f"{len(orphan_cand)} orphan candidate_key in bridge_candidate_developer"
        assert not orphan_dev, f"{len(orphan_dev)} orphan developer_key in bridge_candidate_developer"

    def test_bridge_candidate_priority_fks_valid(self, gold_conn):
        bridge = _read_table(gold_conn, "bridge_candidate_priority")
        cand = _read_table(gold_conn, "dim_candidate_core")
        pri = _read_table(gold_conn, "dim_priority")

        orphan_cand = set(bridge["candidate_key"].dropna().unique()) - set(cand["candidate_key"].unique())
        orphan_pri = set(bridge["priority_key"].dropna().unique()) - set(pri["priority_key"].unique())
        assert not orphan_cand, f"{len(orphan_cand)} orphan candidate_key in bridge_candidate_priority"
        assert not orphan_pri, f"{len(orphan_pri)} orphan priority_key in bridge_candidate_priority"

    def test_bridge_candidate_geography_fks_valid(self, gold_conn):
        bridge = _read_table(gold_conn, "bridge_candidate_geography")
        cand = _read_table(gold_conn, "dim_candidate_core")
        geo = _read_table(gold_conn, "dim_geography")

        orphan_cand = set(bridge["candidate_key"].dropna().unique()) - set(cand["candidate_key"].unique())
        orphan_geo = set(bridge["country_key"].dropna().unique()) - set(geo["country_key"].unique())
        assert not orphan_cand, f"{len(orphan_cand)} orphan candidate_key in bridge_candidate_geography"
        assert not orphan_geo, f"{len(orphan_geo)} orphan country_key in bridge_candidate_geography"

    def test_bridge_trial_geography_fks_valid(self, gold_conn):
        bridge = _read_table(gold_conn, "bridge_trial_geography")
        trial = _read_table(gold_conn, "fact_clinical_trial_event")
        geo = _read_table(gold_conn, "dim_geography")

        orphan_trial = set(bridge["trial_key"].dropna().unique()) - set(trial["trial_id"].unique())
        orphan_geo = set(bridge["country_key"].dropna().unique()) - set(geo["country_key"].unique())
        assert not orphan_trial, f"{len(orphan_trial)} orphan trial_key in bridge_trial_geography"
        assert not orphan_geo, f"{len(orphan_geo)} orphan country_key in bridge_trial_geography"


# ---------------------------------------------------------------------------
# 6. Primary Key Uniqueness
# ---------------------------------------------------------------------------


class TestPrimaryKeyUniqueness:
    """Verify surrogate keys are unique within each table."""

    @pytest.mark.parametrize("table,pk", [
        ("dim_candidate_core", "candidate_key"),
        ("dim_disease", "disease_key"),
        ("dim_phase", "phase_key"),
        ("dim_product", "product_key"),
        ("dim_geography", "country_key"),
        ("dim_organization", "organization_key"),
        ("dim_priority", "priority_key"),
        ("dim_date", "date_key"),
        ("dim_age_group", "age_group_key"),
        ("dim_approving_authority", "authority_key"),
        ("dim_candidate_tech", "technology_key"),
        ("dim_candidate_regulatory", "regulatory_key"),
        ("dim_developer", "developer_key"),
        ("dim_funder", "funder_key"),
        ("fact_pipeline_snapshot", "snapshot_id"),
        ("fact_clinical_trial_event", "trial_id"),
        ("fact_publication", "publication_id"),
    ])
    def test_primary_key_unique(self, gold_conn, table, pk):
        df = _read_table(gold_conn, table)
        duplicates = df[pk].duplicated().sum()
        assert duplicates == 0, f"{duplicates} duplicate {pk} values in {table}"
