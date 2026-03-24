"""Tests for schema_map configuration validation."""

import re

from igh_data_transform.transformations.silver_to_gold.config.phase_sort_order import PHASE_SORT_ORDER
from igh_data_transform.transformations.silver_to_gold.config.schema_map import STAR_SCHEMA_MAP, TABLE_LOAD_ORDER


class TestSchemaMapCompleteness:
    """Test that schema map is complete and valid."""

    def test_all_tables_have_source_or_special(self):
        """Every table must have _source_table or be generated."""
        for table_name, config in STAR_SCHEMA_MAP.items():
            source = config.get("_source_table")
            special = config.get("_special", {})

            has_source = source is not None
            is_generated = special.get("generate", False)
            is_union = source == "UNION"
            is_from_optionset = special.get("from_optionset", False)

            assert has_source or is_generated or is_union or is_from_optionset, (
                f"{table_name} has no _source_table and is not generated, union, or from_optionset"
            )

    def test_dimension_tables_have_pk(self):
        """Dimension tables must have _pk defined."""
        for table_name, config in STAR_SCHEMA_MAP.items():
            if table_name.startswith("dim_"):
                assert "_pk" in config, f"{table_name} is missing _pk"
                assert config["_pk"] is not None, f"{table_name} has None _pk"

    def test_fact_tables_have_pk(self):
        """Fact tables must have _pk defined."""
        for table_name, config in STAR_SCHEMA_MAP.items():
            if table_name.startswith("fact_"):
                assert "_pk" in config, f"{table_name} is missing _pk"

    def test_all_tables_in_load_order(self):
        """All tables in schema map should be in load order."""
        for table_name in STAR_SCHEMA_MAP:
            assert table_name in TABLE_LOAD_ORDER, f"{table_name} in STAR_SCHEMA_MAP but not in TABLE_LOAD_ORDER"

    def test_load_order_tables_in_schema_map(self):
        """All tables in load order should be in schema map."""
        for table_name in TABLE_LOAD_ORDER:
            assert table_name in STAR_SCHEMA_MAP, f"{table_name} in TABLE_LOAD_ORDER but not in STAR_SCHEMA_MAP"

    def test_dimensions_before_facts(self):
        """Dimensions should be loaded before facts."""
        dim_indices = [TABLE_LOAD_ORDER.index(t) for t in TABLE_LOAD_ORDER if t.startswith("dim_")]
        fact_indices = [TABLE_LOAD_ORDER.index(t) for t in TABLE_LOAD_ORDER if t.startswith("fact_")]

        if dim_indices and fact_indices:
            assert max(dim_indices) < min(fact_indices), "Some dimensions are loaded after facts"

    def test_facts_before_bridges(self):
        """Facts should be loaded before bridges."""
        fact_indices = [TABLE_LOAD_ORDER.index(t) for t in TABLE_LOAD_ORDER if t.startswith("fact_")]
        bridge_indices = [TABLE_LOAD_ORDER.index(t) for t in TABLE_LOAD_ORDER if t.startswith("bridge_")]

        if fact_indices and bridge_indices:
            assert max(fact_indices) < min(bridge_indices), "Some facts are loaded after bridges"


class TestPhaseSortOrder:
    """Test phase sort order configuration."""

    def test_phase_sort_order_has_standard_phases(self):
        """Ensure the 10 canonical R&D phases are defined."""
        expected_phases = [
            "Discovery & Preclinical",
            "Early development",
            "Phase I",
            "Phase II",
            "Phase III",
            "Late development",
            "Regulatory filing",
            "Approved",
            "Post-marketing surveillance",
            "Human safety & efficacy",
        ]
        for phase in expected_phases:
            assert phase in PHASE_SORT_ORDER, f"Missing standard phase: {phase}"

    def test_phase_sort_order_is_ordered(self):
        """Ensure phases are in logical order."""
        # Discovery should come before clinical phases
        assert PHASE_SORT_ORDER["Discovery & Preclinical"] < PHASE_SORT_ORDER["Phase I"]

        # Clinical phases should be in order
        assert PHASE_SORT_ORDER["Phase I"] < PHASE_SORT_ORDER["Phase II"]
        assert PHASE_SORT_ORDER["Phase II"] < PHASE_SORT_ORDER["Phase III"]
        assert PHASE_SORT_ORDER["Phase III"] < PHASE_SORT_ORDER["Approved"]

    def test_not_applicable_at_end(self):
        """Not applicable should have high sort order."""
        assert PHASE_SORT_ORDER["Not applicable"] > 900


class TestSchemaMapExpressions:
    """Test schema map expression formats."""

    def test_coalesce_expressions_valid(self):
        """COALESCE expressions should be properly formatted."""
        # Pattern for COALESCE with string default: COALESCE(col, 'value')
        coalesce_string_pattern = r"COALESCE\([^,]+,\s*'[^']*'\)"
        # Pattern for COALESCE with numeric default: COALESCE(col, 0)
        coalesce_numeric_pattern = r"COALESCE\([^,]+,\s*\d+\)"
        # Pattern for COALESCE with two column references: COALESCE(col1, col2)
        coalesce_two_col_pattern = r"COALESCE\(\w+,\s*\w+\)"

        for table_name, config in STAR_SCHEMA_MAP.items():
            for col_name, expr in config.items():
                if col_name.startswith("_") or expr is None:
                    continue
                if isinstance(expr, str) and expr.startswith("COALESCE"):
                    # Should match pattern or be COALESCE(NULL, 'value')
                    valid = (
                        re.match(coalesce_string_pattern, expr)
                        or re.match(coalesce_numeric_pattern, expr)
                        or re.match(coalesce_two_col_pattern, expr)
                        or "COALESCE(NULL," in expr
                    )
                    assert valid, f"Invalid COALESCE in {table_name}.{col_name}: {expr}"

    def test_optionset_expressions_valid(self):
        """OPTIONSET expressions should reference column names."""
        for table_name, config in STAR_SCHEMA_MAP.items():
            for col_name, expr in config.items():
                if col_name.startswith("_") or expr is None:
                    continue
                if isinstance(expr, str) and expr.startswith("OPTIONSET:"):
                    col_ref = expr[len("OPTIONSET:") :]
                    assert col_ref, f"Empty OPTIONSET reference in {table_name}.{col_name}"

    def test_new_candidate_columns_present(self):
        """dim_candidate_core has technology_principle and target_population."""
        config = STAR_SCHEMA_MAP["dim_candidate_core"]
        assert "technology_principle" in config
        assert config["technology_principle"] == "technologyprinciple"
        assert "target_population" in config
        assert config["target_population"] == "new_targetpopulation"

    def test_new_priority_columns_present(self):
        """dim_priority has all new columns including product FK."""
        config = STAR_SCHEMA_MAP["dim_priority"]
        assert "type_of_guidance" not in config, "type_of_guidance was removed; priority_name now maps to ppctitle"
        assert config["priority_name"] == "ppctitle"
        assert config["author"] == "author"
        assert config["publication_date"] == "publicationdate"
        assert config["target_population"] == "targetpopulation"
        assert config["efficacy"] == "efficacy"
        assert config["safety"] == "safety"
        assert config["source"] == "source"
        assert config["product_key"] == "FK:dim_product.vin_productid|product_value"

    def test_healthcare_facility_level_uses_optionset(self):
        """healthcare_facility_level resolves via OPTIONSET, not raw passthrough."""
        config = STAR_SCHEMA_MAP["dim_candidate_core"]
        expr = config["healthcare_facility_level"]
        assert expr.startswith("OPTIONSET:"), f"healthcare_facility_level should use OPTIONSET resolution, got: {expr}"
        assert "vin_healthcarefacilitylevel" in expr

    def test_new_clinical_trial_columns_present(self):
        """fact_clinical_trial_event has all new columns."""
        config = STAR_SCHEMA_MAP["fact_clinical_trial_event"]
        assert config["funder_type"] == "fundertype"
        assert config["interventions"] == "interventions"
        assert config["outcome_measure"] == "COALESCE(outcomemeasure_primary, outcomemeasure_secondary)"
        assert config["sex"] == "sex"
        assert config["study_design"] == "study_design"
        assert config["ct_results_type"] == "OPTIONSET:ctresultstype|vin_ctresultstype"
        assert config["ct_terminated_reason"] == "ctterminatedreason"

    def test_new_candidate_extract_columns_present(self):
        """dim_candidate_core has 4 new extract custom data columns."""
        config = STAR_SCHEMA_MAP["dim_candidate_core"]
        assert config["route_of_administration"] == "routeofadministration"
        assert config["platform"] == "platform"
        assert config["chim_study"] == "OPTIONSET:chimstudyyesno|new_chimstudyyesno"
        assert config["key_clinical_trial"] == "ctregistrylink"

    def test_fk_expressions_valid(self):
        """FK expressions should have valid format."""
        for table_name, config in STAR_SCHEMA_MAP.items():
            for col_name, expr in config.items():
                if col_name.startswith("_") or expr is None:
                    continue
                if isinstance(expr, str) and expr.startswith("FK:"):
                    # Should have format FK:table.column|source_col
                    parts = expr[3:].split("|")
                    assert len(parts) >= 1, f"Invalid FK in {table_name}.{col_name}: {expr}"
                    dim_ref = parts[0]
                    assert "." in dim_ref, f"FK missing table.column format: {expr}"
