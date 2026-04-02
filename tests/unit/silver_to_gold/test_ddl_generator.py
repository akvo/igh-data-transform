"""Tests for silver_to_gold DDL generator."""

from igh_data_transform.transformations.silver_to_gold.core.ddl_generator import (
    generate_all_ddl,
    generate_create_table,
    infer_column_type,
)
from igh_data_transform.transformations.silver_to_gold.config.schema_map import (
    TABLE_LOAD_ORDER,
)


# =========================================================================
# infer_column_type
# =========================================================================


class TestInferColumnType:
    def test_key_suffix(self):
        assert infer_column_type("candidate_key") == "INTEGER"

    def test_id_suffix(self):
        assert infer_column_type("snapshot_id") == "INTEGER"

    def test_flag_suffix(self):
        assert infer_column_type("is_active_flag") == "INTEGER"

    def test_count_suffix(self):
        assert infer_column_type("countries_approved_count") == "INTEGER"

    def test_exact_sort_order(self):
        assert infer_column_type("sort_order") == "INTEGER"

    def test_exact_year(self):
        assert infer_column_type("year") == "INTEGER"

    def test_exact_quarter(self):
        assert infer_column_type("quarter") == "INTEGER"

    def test_exact_enrollment_count(self):
        assert infer_column_type("enrollment_count") == "INTEGER"

    def test_exact_option_code(self):
        assert infer_column_type("option_code") == "INTEGER"

    def test_text_default(self):
        assert infer_column_type("candidate_name") == "TEXT"

    def test_text_for_date(self):
        assert infer_column_type("full_date") == "TEXT"

    def test_case_insensitive(self):
        assert infer_column_type("Candidate_Key") == "INTEGER"


# =========================================================================
# generate_create_table
# =========================================================================


class TestGenerateCreateTable:
    def test_basic_table(self):
        config = {
            "_source_table": "vin_products",
            "_pk": "product_key",
            "vin_productid": "vin_productid",
            "product_name": "vin_name",
        }
        sql = generate_create_table("dim_product", config)
        assert "CREATE TABLE IF NOT EXISTS dim_product" in sql
        assert "product_key INTEGER PRIMARY KEY AUTOINCREMENT" in sql
        assert "vin_productid TEXT" in sql
        assert "product_name TEXT" in sql

    def test_skips_meta_keys(self):
        config = {
            "_source_table": "some_table",
            "_pk": "my_key",
            "_special": {"some": "thing"},
            "col_a": "source_a",
        }
        sql = generate_create_table("test_table", config)
        assert "_source_table" not in sql
        assert "_special" not in sql
        assert "col_a TEXT" in sql

    def test_no_pk(self):
        config = {
            "_source_table": "junction",
            "_pk": None,
            "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
            "developer_key": "FK:dim_developer.developer_name|DELIMITED_VALUE",
        }
        sql = generate_create_table("bridge_test", config)
        assert "PRIMARY KEY" not in sql
        assert "candidate_key INTEGER" in sql

    def test_skips_none_values(self):
        config = {
            "_pk": "my_key",
            "real_col": "source",
            "null_col": None,
        }
        sql = generate_create_table("test", config)
        assert "real_col" in sql
        assert "null_col" not in sql


# =========================================================================
# generate_all_ddl
# =========================================================================


class TestGenerateAllDdl:
    def test_produces_all_tables(self):
        ddl = generate_all_ddl()
        assert len(ddl) == len(TABLE_LOAD_ORDER)

    def test_all_are_create_table(self):
        for stmt in generate_all_ddl():
            assert stmt.strip().startswith("CREATE TABLE IF NOT EXISTS")
