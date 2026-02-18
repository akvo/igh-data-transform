"""Tests for data cleanup transformation utilities."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    normalize_whitespace,
    rename_columns,
    replace_values,
)


class TestDropColumnsByName:
    """Tests for drop_columns_by_name function."""

    def test_drops_listed_columns_that_exist(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        result = drop_columns_by_name(df, ["a", "c"])
        assert list(result.columns) == ["b"]

    def test_ignores_columns_that_dont_exist(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = drop_columns_by_name(df, ["a", "nonexistent"])
        assert list(result.columns) == ["b"]

    def test_returns_unchanged_when_list_is_empty(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = drop_columns_by_name(df, [])
        assert list(result.columns) == ["a", "b"]

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        drop_columns_by_name(df, ["a"])
        assert list(df.columns) == ["a", "b"]


class TestDropEmptyColumns:
    """Tests for drop_empty_columns function."""

    def test_drops_all_null_columns(self):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [None, None, None],
                "c": ["x", "y", "z"],
            }
        )
        result = drop_empty_columns(df)
        assert list(result.columns) == ["a", "c"]

    def test_preserves_specified_columns(self):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "valid_to": [None, None, None],
            }
        )
        result = drop_empty_columns(df, preserve=["valid_to"])
        assert "valid_to" in result.columns

    def test_keeps_partial_null_columns(self):
        df = pd.DataFrame(
            {
                "a": [1, None, 3],
                "b": [None, None, None],
            }
        )
        result = drop_empty_columns(df)
        assert "a" in result.columns
        assert "b" not in result.columns

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = drop_empty_columns(df)
        assert result.empty

    def test_no_empty_columns(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = drop_empty_columns(df)
        assert list(result.columns) == ["a", "b"]


class TestRenameColumns:
    """Tests for rename_columns function."""

    def test_renames_columns(self):
        df = pd.DataFrame({"old_name": [1, 2]})
        result = rename_columns(df, {"old_name": "new_name"})
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_partial_rename(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = rename_columns(df, {"a": "x"})
        assert list(result.columns) == ["x", "b", "c"]

    def test_empty_mapping(self):
        df = pd.DataFrame({"a": [1]})
        result = rename_columns(df, {})
        assert list(result.columns) == ["a"]

    def test_nonexistent_column_ignored(self):
        df = pd.DataFrame({"a": [1]})
        result = rename_columns(df, {"nonexistent": "new"})
        assert list(result.columns) == ["a"]


class TestNormalizeWhitespace:
    """Tests for normalize_whitespace function."""

    def test_strips_whitespace(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_collapses_multiple_spaces(self):
        assert normalize_whitespace("hello    world") == "hello world"

    def test_removes_html_br(self):
        assert normalize_whitespace("line1<br>line2") == "line1 line2"
        assert normalize_whitespace("line1<BR>line2") == "line1 line2"

    def test_normalizes_unicode_space(self):
        assert normalize_whitespace("hello\xa0world") == "hello world"

    def test_returns_none_for_none(self):
        assert normalize_whitespace(None) is None

    def test_returns_none_for_empty_string(self):
        assert normalize_whitespace("   ") is None

    def test_preserves_html_when_disabled(self):
        result = normalize_whitespace("a<br>b", remove_html=False)
        assert result == "a<br>b"

    def test_handles_nan(self):
        assert normalize_whitespace(float("nan")) is None


class TestReplaceValues:
    """Tests for replace_values function."""

    def test_replaces_values(self):
        df = pd.DataFrame({"status": ["Active", "Inactive", "Active"]})
        result = replace_values(df, "status", {"Active": "A", "Inactive": "I"})
        assert list(result["status"]) == ["A", "I", "A"]

    def test_preserves_unmapped_values(self):
        df = pd.DataFrame({"col": ["a", "b", "c"]})
        result = replace_values(df, "col", {"a": "x"})
        assert list(result["col"]) == ["x", "b", "c"]

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"col": ["a", "b"]})
        replace_values(df, "col", {"a": "x"})
        assert list(df["col"]) == ["a", "b"]

    def test_numeric_replacement(self):
        df = pd.DataFrame({"code": [100, 200, 100]})
        result = replace_values(df, "code", {100: 1, 200: 2})
        assert list(result["code"]) == [1, 2, 1]

    def test_empty_mapping(self):
        df = pd.DataFrame({"col": ["a", "b"]})
        result = replace_values(df, "col", {})
        assert list(result["col"]) == ["a", "b"]
