"""Data cleanup transformation utilities for Bronze to Silver layer."""

import pandas as pd


def drop_columns_by_name(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Drop specific named columns from a DataFrame.

    Args:
        df: Input DataFrame
        columns: List of column names to drop

    Returns:
        DataFrame with specified columns removed. Columns not present are ignored.
    """
    cols_to_drop = [c for c in columns if c in df.columns]
    return df.drop(columns=cols_to_drop)


def drop_empty_columns(
    df: pd.DataFrame,
    preserve: list[str] | None = None,
) -> pd.DataFrame:
    """Remove columns that contain only null values.

    Args:
        df: Input DataFrame
        preserve: Column names to never drop (e.g., ['valid_to'])

    Returns:
        DataFrame with empty columns removed
    """
    preserve = preserve or []
    empty_cols = df.columns[df.isnull().all()].tolist()
    cols_to_drop = [c for c in empty_cols if c not in preserve]
    return df.drop(columns=cols_to_drop)


def rename_columns(
    df: pd.DataFrame,
    mapping: dict[str, str],
) -> pd.DataFrame:
    """Rename columns using a mapping dictionary.

    Args:
        df: Input DataFrame
        mapping: Dict of {old_name: new_name}

    Returns:
        DataFrame with renamed columns
    """
    return df.rename(columns=mapping)


def normalize_whitespace(
    value: str | None,
    remove_html: bool = True,
) -> str | None:
    """Normalize whitespace in a string value.

    Args:
        value: Input string (or None)
        remove_html: If True, remove common HTML artifacts like <br>

    Returns:
        Cleaned string or None if input was None
    """
    if value is None or pd.isna(value):
        return None

    result = str(value)

    if remove_html:
        result = result.replace("<br>", " ").replace("<BR>", " ")

    # Normalize unicode whitespace
    result = result.replace("\xa0", " ").replace("\u00a0", " ")

    # Collapse multiple spaces and strip
    result = " ".join(result.split())

    return result if result else None


def replace_values(
    df: pd.DataFrame,
    column: str,
    mapping: dict,
) -> pd.DataFrame:
    """Replace values in a column using a mapping dictionary.

    Args:
        df: Input DataFrame
        column: Column name to transform
        mapping: Dict of {old_value: new_value}

    Returns:
        DataFrame with replaced values
    """
    df = df.copy()
    df[column] = df[column].replace(mapping)
    return df
