"""Tests for schema_map configuration validation."""

import re

from config.phase_sort_order import PHASE_SORT_ORDER
from config.schema_map import STAR_SCHEMA_MAP, TABLE_LOAD_ORDER


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

            assert has_source or is_generated or is_union, (
                f"{table_name} has no _source_table and is not generated or union"
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
        """Ensure standard R&D phases are defined."""
        expected_phases = [
            "Discovery",
            "Preclinical",
            "Phase I",
            "Phase II",
            "Phase III",
            "Phase IV",
        ]
        for phase in expected_phases:
            assert phase in PHASE_SORT_ORDER, f"Missing standard phase: {phase}"

    def test_phase_sort_order_is_ordered(self):
        """Ensure phases are in logical order."""
        # Discovery should come before clinical phases
        assert PHASE_SORT_ORDER["Discovery"] < PHASE_SORT_ORDER["Phase I"]

        # Clinical phases should be in order
        assert PHASE_SORT_ORDER["Phase I"] < PHASE_SORT_ORDER["Phase II"]
        assert PHASE_SORT_ORDER["Phase II"] < PHASE_SORT_ORDER["Phase III"]
        assert PHASE_SORT_ORDER["Phase III"] < PHASE_SORT_ORDER["Phase IV"]

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

        for table_name, config in STAR_SCHEMA_MAP.items():
            for col_name, expr in config.items():
                if col_name.startswith("_") or expr is None:
                    continue
                if isinstance(expr, str) and expr.startswith("COALESCE"):
                    # Should match pattern or be COALESCE(NULL, 'value')
                    valid = (
                        re.match(coalesce_string_pattern, expr)
                        or re.match(coalesce_numeric_pattern, expr)
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
