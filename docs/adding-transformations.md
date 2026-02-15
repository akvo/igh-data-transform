# Adding Data Transformations

This guide explains how to add new data transformations to the Bronze-to-Silver pipeline. It's written for data analysts with basic Python skills.

## Overview

The transformation pipeline cleans and prepares data:

```
Bronze (raw data) → Silver (cleaned data) → Gold (aggregated views)
```

**Bronze layer**: Raw data from Dataverse with all columns, including many that are empty or have messy values.

**Silver layer**: Cleaned data with:
- Metadata and internal columns removed
- Empty columns removed
- Text values normalized (no extra spaces, HTML tags removed)
- Values standardized (e.g., "Active" and "active" become "Active")
- Columns renamed to friendlier names
- Option set tables deduplicated and labels updated

## How the Pipeline Works

When `bronze_to_silver()` runs, it processes each table in the Bronze database:

1. **Registered tables** (e.g., `vin_candidates`) are dispatched to their dedicated transformer function, which applies table-specific logic.
2. **Unregistered tables** receive generic cleanup via `transform_table()`.
3. **Option set tables** (`_optionset_*`) are either written with transformer-applied changes or copied as-is from Bronze.

The registry lives in `src/igh_data_transform/transformations/bronze_to_silver.py`:

```python
TABLE_REGISTRY: dict[str, dict] = {
    "vin_candidates": {
        "transformer": transform_candidates,
        "option_sets": ["_optionset_new_indicationtype", ...],
    },
    "vin_clinicaltrials": {
        "transformer": transform_clinical_trials,
        "option_sets": ["_optionset_vin_ctstatus"],
    },
    ...
}
```

## Available Cleanup Functions

The toolkit provides 5 functions in `igh_data_transform.transformations.cleanup`:

### 1. `drop_columns_by_name(df, columns)`

Drops specific named columns. Columns not present are silently ignored.

```python
from igh_data_transform.transformations import drop_columns_by_name

df = drop_columns_by_name(df, ["row_id", "json_response", "sync_time"])
```

### 2. `drop_empty_columns(df, preserve=None)`

Removes columns that contain only NULL values.

```python
from igh_data_transform.transformations import drop_empty_columns

# Remove all-null columns, but keep valid_to even if empty
df = drop_empty_columns(df, preserve=["valid_to"])
```

### 3. `rename_columns(df, mapping)`

Renames columns using a dictionary.

```python
from igh_data_transform.transformations import rename_columns

df = rename_columns(df, {
    "vin_name": "name",
    "vin_statuscode": "status",
})
```

### 4. `normalize_whitespace(value, remove_html=True)`

Cleans up a single text value:
- Removes leading/trailing spaces
- Collapses multiple spaces into one
- Removes HTML tags like `<br>`
- Handles special unicode spaces

```python
from igh_data_transform.transformations import normalize_whitespace

normalize_whitespace('  hello   world  ')     # Returns: 'hello world'
normalize_whitespace('line1<br>line2')        # Returns: 'line1 line2'
normalize_whitespace('hello\xa0world')        # Returns: 'hello world'
normalize_whitespace(None)                    # Returns: None
normalize_whitespace('   ')                   # Returns: None
```

### 5. `replace_values(df, column, mapping)`

Replaces values in a column using a dictionary.

```python
from igh_data_transform.transformations import replace_values

df = replace_values(df, 'status', {
    'active': 'Active',
    'ACTIVE': 'Active',
    'Inactive': 'Closed',
})
```

## Adding a Table-Specific Transformer

Follow these steps to add a new transformer for a Bronze table. We'll use a hypothetical `vin_partners` table as an example.

### Step 1: Explore the Data

First, examine the Bronze table to identify what needs cleaning:

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('./data/bronze.db')
df = pd.read_sql_query("SELECT * FROM vin_partners", conn)

# See all columns
print(df.columns.tolist())

# Find empty columns (candidates for removal)
empty_cols = df.columns[df.isnull().all()].tolist()
print(f"Empty columns: {empty_cols}")

# Find metadata columns to drop (internal Dataverse fields)
metadata = [c for c in df.columns if c.startswith(('_', 'crc8b_'))]
print(f"Metadata columns: {metadata}")

# Check unique values in a column
print(df['vin_partnertype'].unique())
```

### Step 2: Create the Transformer Module

Create a new file `src/igh_data_transform/transformations/partners.py`:

```python
"""Partners table transformation (vin_partners)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
    replace_values,
)

# Columns to explicitly drop (metadata, internal fields, sync artifacts)
_COLUMNS_TO_DROP = [
    "row_id",
    "json_response",
    "sync_time",
    "_createdby_value",
    "_modifiedby_value",
    "_ownerid_value",
    "_owningbusinessunit_value",
    "_owninguser_value",
    "statuscode",
    "statecode",
    "importsequencenumber",
    "timezoneruleversionnumber",
]

# Column renames: {bronze_name: silver_name}
_COLUMN_RENAMES = {
    "vin_name": "name",
    "vin_partnerid": "partnerid",
    "vin_partnertype": "partnertype",
    "new_country": "country",
    "_vin_disease_value": "diseasevalue",
}

# Value standardization mappings
_PARTNER_TYPE_MAPPING = {
    "Non-governmental organization": "NGO",
    "Non-Governmental Organization": "NGO",
}


def transform_partners(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_partners table from Bronze to Silver.

    Args:
        df: Raw partners DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames keyed by table name.

    Returns:
        Tuple of (transformed DataFrame, dict of cleaned option sets).
    """
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)
    df = drop_empty_columns(df, preserve=["valid_to"])
    df = rename_columns(df, _COLUMN_RENAMES)

    if "partnertype" in df.columns:
        df = replace_values(df, "partnertype", _PARTNER_TYPE_MAPPING)

    # Return empty dict if no option set changes needed
    return df, {}
```

**Key points about the function signature:**
- Takes `df` (the raw Bronze DataFrame) and `option_sets` (dict of related option set DataFrames)
- Returns a **tuple**: `(transformed_df, cleaned_option_sets_dict)`
- The second element is a dict of `{option_set_table_name: cleaned_df}`. Return `{}` if your transformer doesn't modify any option sets.

### Step 3: Register in `TABLE_REGISTRY`

Edit `src/igh_data_transform/transformations/bronze_to_silver.py`:

```python
# Add the import at the top
from igh_data_transform.transformations.partners import transform_partners

# Add the entry to TABLE_REGISTRY
TABLE_REGISTRY: dict[str, dict] = {
    # ... existing entries ...
    "vin_partners": {
        "transformer": transform_partners,
        "option_sets": [],  # List option set table names if needed
    },
}
```

The `option_sets` list tells the pipeline which option set tables to load from Bronze and pass to your transformer. For example, if your table references `_optionset_vin_partnertype`, include it here.

### Step 4: Export from `__init__.py`

Edit `src/igh_data_transform/transformations/__init__.py`:

```python
from igh_data_transform.transformations.partners import transform_partners

__all__ = [
    # ... existing exports ...
    "transform_partners",
]
```

### Step 5: Write Unit Tests

Create `tests/unit/test_partners.py`. Follow the pattern from existing tests:

```python
"""Unit tests for vin_partners transformation."""

import pandas as pd
import pytest

from igh_data_transform.transformations.partners import transform_partners


class TestTransformPartners:

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "row_id": [1, 2],
            "json_response": ["...", "..."],
            "sync_time": ["2025-01-01", "2025-01-01"],
            "vin_name": ["Partner A", "Partner B"],
            "vin_partnerid": ["id-1", "id-2"],
            "vin_partnertype": ["Non-governmental organization", "Academic"],
            "new_country": ["Switzerland", "Kenya"],
            "valid_to": [None, None],
        })

    def test_drops_metadata_columns(self, sample_df):
        result, _ = transform_partners(sample_df)
        assert "row_id" not in result.columns
        assert "json_response" not in result.columns
        assert "sync_time" not in result.columns

    def test_renames_columns(self, sample_df):
        result, _ = transform_partners(sample_df)
        assert "name" in result.columns
        assert "vin_name" not in result.columns

    def test_standardizes_partner_type(self, sample_df):
        result, _ = transform_partners(sample_df)
        assert "NGO" in result["partnertype"].values
        assert "Non-governmental organization" not in result["partnertype"].values

    def test_returns_tuple(self, sample_df):
        result = transform_partners(sample_df)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_preserves_row_count(self, sample_df):
        result, _ = transform_partners(sample_df)
        assert len(result) == len(sample_df)
```

Run unit tests to verify:

```bash
uv run pytest tests/unit/test_partners.py -v
```

The e2e test suite (`uv run pytest --e2e -v`) will automatically pick up your new table in `TestSilverTransformationCompleteness` since it verifies all non-empty Bronze tables appear in Silver.

## Handling Option Sets

Some tables reference option set tables (e.g., `_optionset_vin_ctstatus`). If your transformer needs to clean up an option set (remove deprecated codes, update labels), follow this pattern:

```python
# Codes to remove from the option set after consolidation
_STATUS_CODES_TO_REMOVE = {100000003, 100000004}

def transform_partners(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:

    # ... transform df ...

    # Clean option sets
    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets and "_optionset_vin_partnerstatus" in option_sets:
        os_df = option_sets["_optionset_vin_partnerstatus"].copy()
        # Remove deprecated codes
        os_df = os_df[~os_df["code"].isin(_STATUS_CODES_TO_REMOVE)]
        cleaned_option_sets["_optionset_vin_partnerstatus"] = os_df.reset_index(drop=True)

    return df, cleaned_option_sets
```

Then list the option set in `TABLE_REGISTRY`:

```python
"vin_partners": {
    "transformer": transform_partners,
    "option_sets": ["_optionset_vin_partnerstatus"],
},
```

## Common Transformation Patterns

### Pattern 1: Standardizing Status Codes

Dataverse often stores status as integers. Map them to readable text:

```python
STATUS_MAPPING = {
    100000000: 'Active',
    100000001: 'Inactive',
    100000002: 'Pending',
}

df = replace_values(df, 'statuscode', STATUS_MAPPING)
```

### Pattern 2: Consolidating Option Set Codes

When multiple codes mean the same thing, consolidate them:

```python
# Map old codes to the canonical code
_CONSOLIDATION = {
    100000003: 100000001,  # Duplicate Treatment -> Treatment
    100000004: 100000000,  # Duplicate Prevention -> Prevention
}

df = replace_values(df, "indicationtype", _CONSOLIDATION)
```

### Pattern 3: Cleaning Names with Prefixes

Remove vendor prefixes from column names:

```python
rename_map = {col: col.replace('vin_', '') for col in df.columns if col.startswith('vin_')}
df = rename_columns(df, rename_map)
```

### Pattern 4: Normalizing All Text Columns

Apply whitespace normalization to all string columns:

```python
from igh_data_transform.transformations import normalize_whitespace

text_columns = df.select_dtypes(include=['object']).columns.tolist()
for col in text_columns:
    df[col] = df[col].apply(normalize_whitespace)
```

### Pattern 5: Filtering Records

Keep only records matching a criteria:

```python
# Filter to pipeline-included candidates
df = df[
    (df["includeinpipeline"] == 100000000)
    | (df["includeinpipeline"] == 100000002)
].copy()
```

## Quick Reference

| Function | Purpose | Example |
|----------|---------|---------|
| `drop_columns_by_name(df, cols)` | Drop specific named columns | `drop_columns_by_name(df, ["row_id", "sync_time"])` |
| `drop_empty_columns(df)` | Remove all-NULL columns | `drop_empty_columns(df, preserve=['valid_to'])` |
| `rename_columns(df, mapping)` | Rename columns | `rename_columns(df, {'old': 'new'})` |
| `normalize_whitespace(value)` | Clean text value | `normalize_whitespace('  hello  ')` -> `'hello'` |
| `replace_values(df, col, mapping)` | Replace values | `replace_values(df, 'status', {1: 'Active'})` |
| `transform_table(df, ...)` | Generic cleanup (unregistered tables) | See `bronze_to_silver.py` |

## Running Tests

```bash
# Unit tests only (default)
uv run pytest -v

# E2e tests against a real Bronze DB
uv run pytest --e2e -v

# All tests
uv run pytest --all -v
```

## Need Help?

- Check existing transformers in `src/igh_data_transform/transformations/` (e.g., `priorities.py` is the simplest)
- Look at test examples in `tests/unit/`
- See e2e tests in `tests/e2e/test_bronze_to_silver_e2e.py`
