"""End-to-end tests for the Bronze-to-Silver transformation pipeline.

These tests exercise the full pipeline against a real Bronze database
synced from the Dataverse API.

Run with:  uv run pytest --e2e -v
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


# ---------------------------------------------------------------------------
# 1. Completeness
# ---------------------------------------------------------------------------

class TestSilverTransformationCompleteness:
    """Verify the Silver DB was created and contains expected tables."""

    def test_silver_db_exists(self, silver_db_path):
        assert silver_db_path.exists()

    def test_silver_has_tables(self, silver_conn):
        tables = _table_names(silver_conn)
        assert len(tables) > 0

    def test_core_tables_present(self, silver_conn):
        tables = _table_names(silver_conn)
        expected = {"vin_candidates", "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities"}
        assert expected.issubset(tables), f"Missing tables: {expected - tables}"

    def test_core_tables_have_rows(self, silver_conn):
        for table in ["vin_candidates", "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities"]:
            df = _read_table(silver_conn, table)
            assert len(df) > 0, f"Table {table} is empty"

    def test_non_empty_bronze_tables_carried_over(self, bronze_db_path, silver_conn):
        """All non-empty Bronze tables should appear in Silver."""
        bronze_conn = sqlite3.connect(str(bronze_db_path))
        bronze_tables = _table_names(bronze_conn)
        silver_tables = _table_names(silver_conn)

        for table in bronze_tables:
            df = pd.read_sql_query(f"SELECT COUNT(*) AS cnt FROM {table}", bronze_conn)  # noqa: S608
            if df["cnt"].iloc[0] > 0:
                assert table in silver_tables, f"Non-empty Bronze table '{table}' missing from Silver"

        bronze_conn.close()


# ---------------------------------------------------------------------------
# 2. Candidates
# ---------------------------------------------------------------------------

class TestCandidatesTransformation:
    """Verify vin_candidates transformations."""

    @pytest.fixture(autouse=True)
    def _load(self, silver_conn):
        self.df = _read_table(silver_conn, "vin_candidates")

    def test_metadata_columns_dropped(self):
        dropped = {"row_id", "json_response", "sync_time"}
        present = set(self.df.columns)
        assert dropped.isdisjoint(present), f"Metadata columns not dropped: {dropped & present}"

    def test_columns_renamed(self):
        assert "candidate_name" in self.df.columns
        assert "candidateid" in self.df.columns
        assert "vin_name" not in self.df.columns
        assert "vin_candidateid" not in self.df.columns

    def test_pipeline_filter_applied(self):
        """Only includeinpipeline 100000000 or 100000002 should remain."""
        assert "includeinpipeline" in self.df.columns
        allowed = {100000000, 100000002}
        actual = set(self.df["includeinpipeline"].dropna().astype(int).unique())
        assert actual.issubset(allowed), f"Unexpected includeinpipeline values: {actual - allowed}"

    def test_temporal_expansion_produces_rd_stage(self):
        assert "RD_stage" in self.df.columns

    def test_temporal_expansion_has_valid_from(self):
        assert "valid_from" in self.df.columns
        assert self.df["valid_from"].notna().any()


# ---------------------------------------------------------------------------
# 3. Clinical Trials
# ---------------------------------------------------------------------------

class TestClinicalTrialsTransformation:
    """Verify vin_clinicaltrials transformations."""

    @pytest.fixture(autouse=True)
    def _load(self, silver_conn):
        self.df = _read_table(silver_conn, "vin_clinicaltrials")

    def test_columns_renamed(self):
        assert "ctphase" in self.df.columns
        assert "study_design" in self.df.columns
        assert "vin_ctphase" not in self.df.columns
        assert "new_studydesign" not in self.df.columns

    def test_phase_standardized(self):
        known_phases = {
            "Phase I", "Phase II", "Phase III", "Phase IV",
            "Phase I/II", "Phase II/III", "Phase III/IV",
            "Observational", "Interventional", "Retrospective", "CHIM",
            "N/A", "Unknown",
        }
        actual = set(self.df["ctphase"].dropna().unique())
        unexpected = actual - known_phases
        assert not unexpected, f"Unexpected phase values: {unexpected}"

    def test_gender_standardized(self):
        known_genders = {"Both", "Male", "Female", "Unknown"}
        actual = set(self.df["sex"].dropna().unique())
        unexpected = actual - known_genders
        assert not unexpected, f"Unexpected gender values: {unexpected}"

    def test_age_groups_standardized(self):
        known_ages = {
            "Neonates", "Infants", "Children", "Adolescents",
            "Young Adults 18 - 45", "Older adults: 45 >", "Unknown",
        }
        actual = set(self.df["age"].dropna().unique())
        unexpected = actual - known_ages
        assert not unexpected, f"Unexpected age group values: {unexpected}"

    def test_metadata_columns_dropped(self):
        dropped = {"json_response", "sync_time"}
        present = set(self.df.columns)
        assert dropped.isdisjoint(present)


# ---------------------------------------------------------------------------
# 4. Diseases
# ---------------------------------------------------------------------------

class TestDiseasesTransformation:
    """Verify vin_diseases transformations."""

    @pytest.fixture(autouse=True)
    def _load(self, silver_conn):
        self.df = _read_table(silver_conn, "vin_diseases")

    def test_columns_renamed(self):
        assert "disease" in self.df.columns
        assert "name" in self.df.columns
        assert "diseaseid" in self.df.columns
        assert "vin_disease" not in self.df.columns
        assert "vin_name" not in self.df.columns

    def test_metadata_columns_dropped(self):
        dropped = {"row_id", "json_response", "sync_time"}
        present = set(self.df.columns)
        assert dropped.isdisjoint(present)


# ---------------------------------------------------------------------------
# 5. Priorities
# ---------------------------------------------------------------------------

class TestPrioritiesTransformation:
    """Verify vin_rdpriorities transformations."""

    @pytest.fixture(autouse=True)
    def _load(self, silver_conn):
        self.df = _read_table(silver_conn, "vin_rdpriorities")

    def test_columns_renamed(self):
        assert "name" in self.df.columns
        assert "author" in self.df.columns
        assert "rdpriorityid" in self.df.columns
        assert "vin_name" not in self.df.columns
        assert "new_author" not in self.df.columns

    def test_who_author_standardized(self):
        if "author" in self.df.columns:
            authors = self.df["author"].dropna().unique()
            assert "World Health Organization" not in authors, (
                "'World Health Organization' should be mapped to 'WHO'"
            )

    def test_metadata_columns_dropped(self):
        dropped = {"row_id", "json_response", "sync_time"}
        present = set(self.df.columns)
        assert dropped.isdisjoint(present)


# ---------------------------------------------------------------------------
# 6. Option Sets
# ---------------------------------------------------------------------------

class TestOptionSetTransformation:
    """Verify option set tables in Silver."""

    def test_option_sets_present(self, silver_conn):
        tables = _table_names(silver_conn)
        option_sets = {t for t in tables if t.startswith("_optionset_")}
        assert len(option_sets) > 0, "No option set tables found in Silver"

    def test_global_health_area_label_updated(self, silver_conn):
        tables = _table_names(silver_conn)
        if "_optionset_new_globalhealtharea" not in tables:
            pytest.skip("_optionset_new_globalhealtharea not in Silver")

        df = _read_table(silver_conn, "_optionset_new_globalhealtharea")
        labels = df["label"].tolist()
        assert "Sexual & reproductive health" not in labels, (
            "'Sexual & reproductive health' should be renamed to 'Womens Health'"
        )
        assert "Womens Health" in labels

    def test_indication_type_duplicates_removed(self, silver_conn):
        tables = _table_names(silver_conn)
        if "_optionset_new_indicationtype" not in tables:
            pytest.skip("_optionset_new_indicationtype not in Silver")

        df = _read_table(silver_conn, "_optionset_new_indicationtype")
        codes = set(df["code"].astype(int).tolist())
        deprecated = {100000003, 100000004, 100000005}
        assert deprecated.isdisjoint(codes), (
            f"Deprecated indication type codes still present: {deprecated & codes}"
        )


# ---------------------------------------------------------------------------
# 7. Data Integrity
# ---------------------------------------------------------------------------

class TestDataIntegrity:
    """Cross-table data integrity checks."""

    # -- No null business keys --

    def test_candidates_no_null_candidateid(self, silver_conn):
        df = _read_table(silver_conn, "vin_candidates")
        nulls = df["candidateid"].isna().sum()
        assert nulls == 0, f"{nulls} null candidateid values in vin_candidates"

    def test_clinical_trials_no_null_clinicaltrialid(self, silver_conn):
        df = _read_table(silver_conn, "vin_clinicaltrials")
        nulls = df["clinicaltrialid"].isna().sum()
        assert nulls == 0, f"{nulls} null clinicaltrialid values in vin_clinicaltrials"

    def test_diseases_no_null_diseaseid(self, silver_conn):
        df = _read_table(silver_conn, "vin_diseases")
        nulls = df["diseaseid"].isna().sum()
        assert nulls == 0, f"{nulls} null diseaseid values in vin_diseases"

    def test_priorities_no_null_rdpriorityid(self, silver_conn):
        df = _read_table(silver_conn, "vin_rdpriorities")
        nulls = df["rdpriorityid"].isna().sum()
        assert nulls == 0, f"{nulls} null rdpriorityid values in vin_rdpriorities"

    # -- Referential integrity --

    def test_clinical_trials_reference_valid_candidates(self, silver_conn, bronze_db_path):
        """candidate_value in clinical trials must reference a candidateid that
        exists in the Bronze source.  Silver vin_candidates is filtered by
        ``includeinpipeline``, so we validate against the unfiltered Bronze set."""
        ct = _read_table(silver_conn, "vin_clinicaltrials")

        if "candidate_value" not in ct.columns:
            pytest.skip("candidate_value column not in vin_clinicaltrials")

        bronze_conn = sqlite3.connect(str(bronze_db_path))
        bronze_cand = pd.read_sql_query(
            "SELECT vin_candidateid FROM vin_candidates", bronze_conn,
        )
        bronze_conn.close()

        ct_refs = set(ct["candidate_value"].dropna().unique())
        valid_ids = set(bronze_cand["vin_candidateid"].dropna().unique())
        orphans = ct_refs - valid_ids
        assert not orphans, (
            f"{len(orphans)} clinical trial records reference non-existent candidates "
            f"(not even in Bronze): {list(orphans)[:10]}..."
        )

    def test_candidates_reference_valid_diseases(self, silver_conn):
        """diseasevalue in candidates must reference existing diseaseid."""
        cand = _read_table(silver_conn, "vin_candidates")
        diseases = _read_table(silver_conn, "vin_diseases")

        if "diseasevalue" not in cand.columns:
            pytest.skip("diseasevalue column not in vin_candidates")

        cand_refs = set(cand["diseasevalue"].dropna().unique())
        valid_ids = set(diseases["diseaseid"].dropna().unique())
        orphans = cand_refs - valid_ids
        assert not orphans, (
            f"{len(orphans)} candidate records reference non-existent diseases: "
            f"{list(orphans)[:10]}..."
        )

    def test_priorities_reference_valid_diseases(self, silver_conn):
        """diseasevalue in priorities must reference existing diseaseid."""
        pri = _read_table(silver_conn, "vin_rdpriorities")
        diseases = _read_table(silver_conn, "vin_diseases")

        if "diseasevalue" not in pri.columns:
            pytest.skip("diseasevalue column not in vin_rdpriorities")

        pri_refs = set(pri["diseasevalue"].dropna().unique())
        valid_ids = set(diseases["diseaseid"].dropna().unique())
        orphans = pri_refs - valid_ids
        assert not orphans, (
            f"{len(orphans)} priority records reference non-existent diseases: "
            f"{list(orphans)[:10]}..."
        )

    # -- Temporal consistency --

    @pytest.mark.parametrize("table", [
        "vin_candidates", "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities",
    ])
    def test_valid_from_not_null(self, silver_conn, table):
        df = _read_table(silver_conn, table)
        if "valid_from" not in df.columns:
            pytest.skip(f"valid_from column not in {table}")
        nulls = df["valid_from"].isna().sum()
        assert nulls == 0, f"{nulls} null valid_from values in {table}"

    @pytest.mark.parametrize("table", [
        "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities",
    ])
    def test_current_records_exist(self, silver_conn, table):
        """At least some records should have valid_to IS NULL (current).

        vin_candidates is excluded because temporal expansion always assigns
        a concrete valid_to date for every year-slice row.
        """
        df = _read_table(silver_conn, table)
        if "valid_to" not in df.columns:
            pytest.skip(f"valid_to column not in {table}")
        current = df["valid_to"].isna().sum()
        assert current > 0, f"No current records (valid_to IS NULL) in {table}"

    @pytest.mark.parametrize("table", [
        "vin_candidates", "vin_clinicaltrials", "vin_diseases", "vin_rdpriorities",
    ])
    def test_no_invalid_temporal_ranges(self, silver_conn, table):
        """No records where valid_to < valid_from."""
        df = _read_table(silver_conn, table)
        if "valid_from" not in df.columns or "valid_to" not in df.columns:
            pytest.skip(f"Temporal columns missing in {table}")

        # Only check rows where both are non-null
        mask = df["valid_from"].notna() & df["valid_to"].notna()
        subset = df[mask].copy()
        if subset.empty:
            return

        subset["vf"] = pd.to_datetime(subset["valid_from"])
        subset["vt"] = pd.to_datetime(subset["valid_to"])
        invalid = subset[subset["vt"] < subset["vf"]]
        assert len(invalid) == 0, (
            f"{len(invalid)} records in {table} have valid_to < valid_from"
        )
