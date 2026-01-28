# Adding Data Transformations

This guide explains how to add new data transformations to the Bronze-to-Silver pipeline. It's written for data analysts with basic Python skills.

## Overview

The transformation pipeline cleans and prepares data:

```
Bronze (raw data) → Silver (cleaned data) → Gold (aggregated views)
```

**Bronze layer**: Raw data from Dataverse with all columns, including many that are empty or have messy values.

**Silver layer**: Cleaned data with:
- Empty columns removed
- Text values normalized (no extra spaces, HTML tags removed)
- Values standardized (e.g., "Active" and "active" become "Active")
- Columns renamed to friendlier names

## Available Cleanup Functions

The toolkit provides 4 simple functions you can use:

### 1. `drop_empty_columns(df, preserve=None)`

Removes columns that contain only NULL values.

```python
import pandas as pd
from igh_data_transform.transformations import drop_empty_columns

df = pd.DataFrame({
    'name': ['Alice', 'Bob'],
    'email': [None, None],      # This column will be removed
    'valid_to': [None, None],   # This will be preserved by default
})

result = drop_empty_columns(df)
# Result has columns: ['name', 'valid_to']

# To preserve additional columns:
result = drop_empty_columns(df, preserve=['valid_to', 'email'])
# Result has columns: ['name', 'email', 'valid_to']
```

### 2. `rename_columns(df, mapping)`

Renames columns using a dictionary.

```python
from igh_data_transform.transformations import rename_columns

df = pd.DataFrame({
    'vin_candidatename': ['Product A', 'Product B'],
    'vin_statuscode': [1, 2],
})

result = rename_columns(df, {
    'vin_candidatename': 'name',
    'vin_statuscode': 'status',
})
# Result has columns: ['name', 'status']
```

### 3. `normalize_whitespace(value, remove_html=True)`

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

### 4. `replace_values(df, column, mapping)`

Replaces values in a column using a dictionary.

```python
from igh_data_transform.transformations import replace_values

df = pd.DataFrame({
    'status': ['Active', 'Inactive', 'active', 'ACTIVE'],
})

result = replace_values(df, 'status', {
    'active': 'Active',
    'ACTIVE': 'Active',
    'Inactive': 'Closed',
})
# Result: ['Active', 'Closed', 'Active', 'Active']
```

## Using `transform_table()` for Standard Cleanup

The `transform_table()` function combines all cleanup steps:

```python
from igh_data_transform.transformations import transform_table

df = pd.DataFrame({
    'vin_name': ['  Product A  ', 'Product<br>B'],
    'empty_col': [None, None],
    'status': ['Active', 'Inactive'],
    'valid_to': [None, None],
})

result = transform_table(
    df,
    column_renames={'vin_name': 'name'},
    text_columns=['name'],
    value_mappings={'status': {'Inactive': 'Closed'}},
    preserve_columns=['valid_to'],
)

# Result:
# - 'empty_col' is removed (all NULL)
# - 'vin_name' renamed to 'name'
# - 'name' values cleaned: 'Product A', 'Product B'
# - 'status' values: 'Active', 'Closed'
# - 'valid_to' preserved even though empty
```

## Adding Table-Specific Transformations

To add custom transformations for a specific table, you'll create a configuration. Here's the pattern:

### Step 1: Identify What Needs Cleaning

First, explore your data in a Jupyter notebook or Python script:

```python
import sqlite3
import pandas as pd

# Connect to the Bronze database
conn = sqlite3.connect('./data/bronze.db')

# Load a table
df = pd.read_sql_query("SELECT * FROM vin_candidates WHERE valid_to IS NULL", conn)

# See all columns
print(df.columns.tolist())

# Check for empty columns
empty_cols = df.columns[df.isnull().all()].tolist()
print(f"Empty columns: {empty_cols}")

# Check unique values in a column
print(df['vin_statuscode'].unique())

# Look at text values that might need cleaning
print(df['vin_name'].head(20))
```

### Step 2: Define Your Transformation Config

Create a dictionary that describes what to clean:

```python
# Example configuration for the vin_candidates table
VIN_CANDIDATES_CONFIG = {
    # Columns to rename (old_name: new_name)
    'column_renames': {
        'vin_candidatename': 'name',
        'vin_statuscode': 'status',
        'vin_phase': 'phase',
        'vin_includeinpipeline': 'include_in_pipeline',
    },

    # Text columns to normalize whitespace
    'text_columns': [
        'name',
        'description',
    ],

    # Value replacements (column: {old: new})
    'value_mappings': {
        'status': {
            100000000: 'Active',
            100000001: 'Inactive',
            100000002: 'On Hold',
        },
        'include_in_pipeline': {
            100000000: True,
            100000001: False,
        },
    },

    # Columns to keep even if empty
    'preserve_columns': ['valid_from', 'valid_to'],
}
```

### Step 3: Apply the Transformation

```python
from igh_data_transform.transformations import transform_table

# Apply the configuration
df_clean = transform_table(
    df,
    column_renames=VIN_CANDIDATES_CONFIG['column_renames'],
    text_columns=VIN_CANDIDATES_CONFIG['text_columns'],
    value_mappings=VIN_CANDIDATES_CONFIG['value_mappings'],
    preserve_columns=VIN_CANDIDATES_CONFIG['preserve_columns'],
)

# Save to Silver database
silver_conn = sqlite3.connect('./data/silver.db')
df_clean.to_sql('candidates', silver_conn, if_exists='replace', index=False)
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

### Pattern 2: Cleaning Names with Prefixes

Remove vendor prefixes from column names:

```python
# Remove 'vin_' prefix from all columns
rename_map = {col: col.replace('vin_', '') for col in df.columns if col.startswith('vin_')}
df = rename_columns(df, rename_map)
```

### Pattern 3: Handling Boolean Fields

Convert integer flags to actual booleans:

```python
df = replace_values(df, 'is_active', {
    100000000: True,
    100000001: False,
    1: True,
    0: False,
})
```

### Pattern 4: Normalizing All Text Columns

Apply whitespace normalization to all string columns:

```python
from igh_data_transform.transformations import normalize_whitespace

text_columns = df.select_dtypes(include=['object']).columns.tolist()
for col in text_columns:
    df[col] = df[col].apply(normalize_whitespace)
```

### Pattern 5: Filtering Current Records Only

Work only with current (non-historical) records:

```python
# Filter to current records only (valid_to is NULL)
df_current = df[df['valid_to'].isnull()].copy()
```

## Testing Your Transformations

Always verify your transformations work correctly:

```python
# Before transformation
print("Before:")
print(f"  Rows: {len(df)}")
print(f"  Columns: {len(df.columns)}")
print(f"  Sample values: {df['status'].unique()[:5]}")

# Apply transformation
df_clean = transform_table(df, ...)

# After transformation
print("\nAfter:")
print(f"  Rows: {len(df_clean)}")
print(f"  Columns: {len(df_clean.columns)}")
print(f"  Sample values: {df_clean['status'].unique()[:5]}")

# Check for unexpected NULLs
null_counts = df_clean.isnull().sum()
print(f"\nNull counts:\n{null_counts[null_counts > 0]}")
```

## Quick Reference

| Function | Purpose | Example |
|----------|---------|---------|
| `drop_empty_columns(df)` | Remove all-NULL columns | `drop_empty_columns(df, preserve=['valid_to'])` |
| `rename_columns(df, mapping)` | Rename columns | `rename_columns(df, {'old': 'new'})` |
| `normalize_whitespace(value)` | Clean text value | `normalize_whitespace('  hello  ')` → `'hello'` |
| `replace_values(df, col, mapping)` | Replace values | `replace_values(df, 'status', {1: 'Active'})` |
| `transform_table(df, ...)` | Apply all cleanup steps | See examples above |

## Need Help?

- Check existing transformations in `src/igh_data_transform/transformations/`
- Look at test examples in `tests/unit/test_cleanup.py`
- Run tests with `uv run pytest -v`
