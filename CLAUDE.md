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

### Planned CLI Commands

```bash
# Transform Bronze to Silver (normalized tables)
igh-transform bronze-to-silver --bronze-db <path> --silver-db <path>

# Transform Silver to Gold (materialized views)
igh-transform silver-to-gold --silver-db <path>
```

### Package Structure

```
src/igh_data_transform/
├── __init__.py
├── cli.py                    # CLI entry point
├── transformations/
│   ├── __init__.py
│   ├── bronze_to_silver.py   # Orchestrates Bronze → Silver pipeline
│   ├── candidates.py         # vin_candidates table transformer
│   ├── cleanup.py            # Shared cleanup utilities (drop columns, rename, normalize, etc.)
│   ├── clinical_trials.py    # vin_clinical_trials table transformer
│   ├── diseases.py           # vin_diseases table transformer
│   ├── priorities.py         # vin_priorities table transformer
│   └── silver_to_gold.py     # Silver → Gold pipeline (stub)
└── utils/
    └── database.py           # SQLite connection utilities
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

# E2e tests only (requires a Bronze DB or Dataverse credentials)
uv run pytest --e2e -v

# All tests including e2e
uv run pytest --all -v

# E2e with a pre-existing Bronze DB
E2E_BRONZE_DB_PATH=/path/to/bronze.db uv run pytest --e2e -v
```

**E2e test Bronze DB resolution order:**
1. `E2E_BRONZE_DB_PATH` env var (if set)
2. Cached `tests/data/bronze.db` (if exists with core tables)
3. Auto-sync via `sync-dataverse` (requires `DATAVERSE_API_URL`, `DATAVERSE_CLIENT_ID`, `DATAVERSE_CLIENT_SECRET`, `DATAVERSE_SCOPE`)
4. Skip with helpful message if none available

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
