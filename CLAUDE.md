# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python package for IGH data transformation. The project uses UV for dependency management and packaging.

**Distribution:** Python library package installable via pip/uv, with a CLI entry point `igh-transform`.

**Architecture Context:** See `../IGH_Data_Pipeline_LLD.md` for the complete Low-Level Design document.

### Data Pipeline Architecture

This package is part of a multi-stage data pipeline:

1. **Bronze Layer** (igh-data-sync): Raw Dataverse data with SCD2 temporal tracking
2. **Silver Layer** (this package): Normalized, cleaned dimension and fact tables
3. **Gold Layer** (this package): Pre-aggregated materialized views for analytics

### CLI Commands

```bash
# Transform Bronze to Silver (normalized tables)
igh-transform bronze-to-silver --bronze-db <path> --silver-db <path>

# Transform Silver to Gold (star schema)
igh-transform silver-to-gold --silver-db <path> --gold-db <path>
```

### Package Structure

```
src/igh_data_transform/
├── __init__.py
├── cli.py                    # CLI entry point
├── transformations/
│   ├── __init__.py
│   ├── bronze_to_silver.py   # Orchestrates Bronze → Silver pipeline
│   ├── _candidates_config.py # Column-level config for candidates
│   ├── candidates.py         # vin_candidates table transformer
│   ├── cleanup.py            # Shared cleanup utilities
│   ├── clinical_trials.py    # vin_clinicaltrials table transformer
│   ├── developers.py         # vin_developers table transformer
│   ├── diseases.py           # vin_diseases table transformer
│   ├── priorities.py         # vin_rdpriorities table transformer
│   └── silver_to_gold/       # Silver → Gold star schema ETL
│       ├── __init__.py       # Public API: silver_to_gold()
│       ├── config/
│       │   ├── schema_map.py         # Declarative table/column mappings
│       │   ├── phase_sort_order.py   # R&D phase ordering
│       │   └── country_iso_codes.py  # ISO 3166 code lookups
│       └── core/
│           ├── main.py        # ETL orchestrator (run_etl)
│           ├── extractor.py   # Read-only source DB access
│           ├── transformer.py # Expression evaluation, FK resolution
│           ├── loader.py      # Target DB schema + data loading
│           ├── ddl_generator.py # CREATE TABLE from schema_map
│           ├── expressions.py # COALESCE, CASE WHEN, LOOKUP parsers
│           ├── bridges.py     # Bridge table transformations
│           ├── dimensions.py  # Date generation, phase post-processing
│           └── year_expansion.py # SCD2 year infill
└── utils/
    ├── database.py           # SQLite connection utilities
    └── country_aliases.py    # Country name variant mappings
```

### Test Structure

```
tests/
├── conftest.py               # Shared fixtures + --e2e/--all CLI hooks
├── unit/                     # Fast unit tests (mocked data)
│   ├── test_bronze_to_silver.py
│   ├── test_candidates.py
│   ├── test_clinical_trials.py
│   ├── test_cleanup.py
│   ├── test_diseases.py
│   ├── test_priorities.py
│   └── ...
├── e2e/                      # End-to-end tests (real Bronze DB)
│   ├── conftest.py           # Session-scoped Bronze/Silver DB fixtures
│   └── test_bronze_to_silver_e2e.py
└── data/                     # Cached Bronze DB (gitignored)
```

## Development Commands

### Environment Setup

**Using UV (Recommended):**
```bash
# Install UV if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Python and dependencies (UV auto-installs Python 3.10 if needed)
uv sync

# Install with dev dependencies (when available)
uv sync --all-extras
```

**Using pip (Alternative):**
```bash
# Install in editable mode
pip install -e .

# Install with dev dependencies (when available)
pip install -e .[dev]
```

### Running the CLI

**Recommended - using `uv run` (no venv activation needed):**
```bash
# Run the CLI directly
uv run igh-transform

# Run Python scripts directly
uv run python -m igh_data_transform
```

**Alternative - activate virtual environment first:**
```bash
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate     # On Windows

# Then run normally
igh-transform
```

### Development Workflow

Common UV commands:
- **Add a dependency**: `uv add <package-name>`
- **Add a dev dependency**: `uv add --dev <package-name>`
- **Update dependencies**: `uv sync`
- **Run commands without activating venv**: `uv run <command>`
- **Run Python scripts**: `uv run python <script.py>`

### Running Tests

```bash
# Unit tests only (default — e2e tests are excluded)
uv run pytest -v

# E2e tests (requires E2E_BRONZE_DB_PATH pointing at a fully populated Bronze DB)
E2E_BRONZE_DB_PATH=/path/to/bronze.db uv run pytest --e2e -v

# All tests including e2e
E2E_BRONZE_DB_PATH=/path/to/bronze.db uv run pytest --all -v
```

E2e tests are skipped when `E2E_BRONZE_DB_PATH` is not set.

## Project Structure

**src/igh_data_transform/** - Main package source code
- `__init__.py` - Package initialization and main() entry point

**Configuration:**
- `pyproject.toml` - Project metadata, dependencies, and build configuration
- `uv.lock` - Locked dependency versions
- `.python-version` - Python version specification (3.10)

## Development Notes

- Python version: >=3.10 (automatically managed by UV via `.python-version` file)
- Build system: uv_build
- Package is editable-installed by default during development
- UV handles Python installation - no need to pre-install Python
