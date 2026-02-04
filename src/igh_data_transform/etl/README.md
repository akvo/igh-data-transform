# ETL Pipeline

Transforms Dataverse raw schema into OLAP star schema.

## Quick Start

**Prerequisites:** Python 3.10+

```bash
cd etl
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

**Run the ETL:**
```bash
python -m src.main --source ../dataverse_complete.db --output output/star_schema.db
```

**Options:**
- `--source PATH` - Path to source Dataverse database (required)
- `--output PATH` - Path to output star schema database (required)
- `-v, --verbose` - Enable debug logging

**Output:** Creates SQLite database at the specified output path. Existing file is overwritten.

## Architecture

```
Source DB (Dataverse) ──► Extractor ──► Transformer ──► Loader ──► Target DB (Star Schema)
                              │              │
                              ▼              ▼
                        Optionset Cache   Dimension Key Cache
```

| Module | Responsibility |
|--------|---------------|
| `src/main.py` | Orchestrates pipeline, handles CLI args, coordinates table load order |
| `src/extractor.py` | Read-only access to source DB, builds optionset cache for code→label lookups |
| `src/transformer.py` | Applies schema_map expressions, caches dimension keys for FK resolution |
| `src/loader.py` | Creates target schema, inserts data, verifies FK integrity |
| `src/ddl_generator.py` | Generates CREATE TABLE from schema_map using naming conventions for types |
| `config/schema_map.py` | **Source of truth** - declares all table mappings and load order |

**Design rationale:** Separation of concerns (extract/transform/load) enables testing each phase independently. Declarative config in schema_map.py means transformations are documented by their definition.

## Schema Map Reference

Location: [`config/schema_map.py`](config/schema_map.py)

The schema map is the "Rosetta Stone" - every target column traces back to its source through a declarative expression.

### Expression Types

| Type | Syntax | Example |
|------|--------|---------|
| Simple column | `"source_col"` | `"vin_name"` |
| COALESCE | `"COALESCE(col, 'default')"` | `"COALESCE(new_platform, 'Unknown')"` |
| CASE WHEN | `"CASE WHEN col = val THEN x ELSE y END"` | `"CASE WHEN statecode = 0 THEN 1 ELSE 0 END"` |
| OPTIONSET | `"OPTIONSET:column_name"` | `"OPTIONSET:vin_approvalstatus"` |
| FK | `"FK:dim_table.lookup_col\|source_col"` | `"FK:dim_product.vin_productid\|_vin_mainproduct_value"` |
| FK (composite) | `"FK:dim_table.COMPOSITE\|col1,col2"` | `"FK:dim_candidate_tech.COMPOSITE\|new_platform,vin_technologytype"` |
| LOOKUP | `"LOOKUP:LOOKUP_NAME"` | `"LOOKUP:PHASE_SORT_ORDER"` |
| LITERAL | `"LITERAL:value"` | `"LITERAL:Developer"` |

**OPTIONSET** resolves Dataverse integer codes to human-readable labels via `_optionset_*` tables in the source database.

**FK** looks up surrogate keys from dimension caches built during load.

### Table Configuration

Each table in `STAR_SCHEMA_MAP` has:
```python
"target_table": {
    "_source_table": "source_table_name",  # or None for generated, "UNION" for bridges
    "_pk": "primary_key_column",           # auto-increment surrogate key
    "_special": {...},                     # optional: distinct, generate, union_sources
    "target_column": "source_expression",  # mapping per column
}
```

### Special Table Types

| Type | `_special` Config | Example |
|------|------------------|---------|
| DISTINCT dimension | `{"distinct": True, "distinct_cols": [...]}` | `dim_candidate_tech` - unique tech combinations |
| Generated dimension | `{"generate": True, "start_year": 2015, "end_year": 2030}` | `dim_date` - programmatic date spine |
| UNION bridge | `_source_table: "UNION"` + `{"union_sources": [...]}` | `bridge_candidate_geography` - multiple source tables |

### Load Order

`TABLE_LOAD_ORDER` in schema_map.py defines execution sequence:

1. **Dimensions** - loaded first, keys cached for FK lookups
2. **Facts** - depend on dimension keys
3. **Bridges** - depend on dimension keys

Order matters because FK resolution requires the target dimension to be loaded and cached first.

## Adding/Modifying Tables

### Add a new dimension

1. Add entry to `STAR_SCHEMA_MAP` in `config/schema_map.py`
2. Add table name to `TABLE_LOAD_ORDER` (before facts)
3. If FK lookups needed: add natural key mapping to `_find_lookup_column()` in `main.py`
4. Run tests: `pytest`

### Add a new fact/bridge

1. Add entry to `STAR_SCHEMA_MAP` with FK expressions
2. Add table name to `TABLE_LOAD_ORDER` (after dimensions)
3. Add FK checks to `verify_foreign_keys()` in `loader.py`
4. Run tests: `pytest`

### Modify column mapping

1. Update expression in `STAR_SCHEMA_MAP`
2. If new expression type needed, add parser to `transformer.py`
3. Run tests: `pytest`

## Testing

```bash
cd etl
pytest -v
```

**Test coverage:**
- `tests/test_schema_map.py` - validates schema_map structure, load order constraints
- `tests/test_transformations.py` - unit tests for expression parsing (COALESCE, CASE WHEN, OPTIONSET), key caching, date dimension generation

Run with verbose output:
```bash
pytest -v --tb=short
```

## Code Quality

Pre-commit hooks run automatically on commit. To run manually:

```bash
pre-commit run --all-files
```

**Tools:**
- **Ruff** - Linting and formatting (line-length=120)
- **Pylint** - File length enforcement (max 400 lines per module)
- **MyPy** - Type checking (optional, not in pre-commit)
- **Pytest** - Runs tests before commit

Run linters individually:
```bash
ruff check src/ config/ tests/
ruff format --check src/ config/ tests/
pylint src/ config/ tests/
mypy src/ config/
```
