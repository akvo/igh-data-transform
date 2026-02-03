# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python package for IGH data transformation. The project uses UV for dependency management and packaging.

**Distribution:** Python library package installable via pip/uv, with a CLI entry point `igh-data-transform`.

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

### Planned Package Structure

```
src/igh_data_transform/
├── __init__.py
├── cli.py                    # CLI entry point
├── transformations/
│   ├── dimensions.py         # dim_candidates, dim_products, dim_diseases, etc.
│   ├── facts.py              # fact_candidate_history, fact_clinical_trials
│   └── aggregations.py       # Gold layer materialized views
├── utils/
│   └── database.py           # SQLite connection utilities
sql/
├── silver/                   # SQL templates for Silver tables
└── gold/                     # SQL templates for Gold views
```

## Development Commands

### Environment Setup

**Using UV (Recommended):**
```bash
# Install UV if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Python and dependencies (UV auto-installs Python 3.12 if needed)
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
uv run igh-data-transform

# Run Python scripts directly
uv run python -m igh_data_transform
```

**Alternative - activate virtual environment first:**
```bash
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate     # On Windows

# Then run normally
igh-data-transform
```

### Development Workflow

Common UV commands:
- **Add a dependency**: `uv add <package-name>`
- **Add a dev dependency**: `uv add --dev <package-name>`
- **Update dependencies**: `uv sync`
- **Run commands without activating venv**: `uv run <command>`
- **Run Python scripts**: `uv run python <script.py>`

## Project Structure

**src/igh_data_transform/** - Main package source code
- `__init__.py` - Package initialization and main() entry point

**Configuration:**
- `pyproject.toml` - Project metadata, dependencies, and build configuration
- `uv.lock` - Locked dependency versions
- `.python-version` - Python version specification (3.12)

## Development Notes

- Python version: 3.12 (automatically managed by UV via `.python-version` file)
- Build system: uv_build
- Package is editable-installed by default during development
- UV handles Python installation - no need to pre-install Python
