"""Tests for developers transformation."""

import pandas as pd

from igh_data_transform.transformations.developers import (
    _enrich_from_accounts,
    transform_developers,
)


def _make_countries():
    """Minimal vin_countries lookup table."""
    return pd.DataFrame(
        {
            "vin_countryno": [1, 2, 3, 4],
            "vin_name": ["Turkey", "United States of America", "South Korea", "France"],
        }
    )


def _make_accounts(**overrides):
    """Minimal accounts table with optional overrides."""
    data = {
        "accountid": ["acc-1", "acc-2"],
        "name": ["Acme Corp", "BioTech Inc"],
        "vin_organisationtype": ["For Profit SME", "5001"],
        "address1_country": ["France", "Türkiye"],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def _make_developers(**overrides):
    """Minimal vin_developers DataFrame."""
    data = {
        "vin_developerid": ["dev-1", "dev-2"],
        "vin_name": ["ProductA - Acme Corp", "ProductB - BioTech Inc"],
        "_vin_cap_value": ["cand-1", "cand-2"],
        "_vin_developer_value": ["acc-1", "acc-2"],
        "statecode": [0, 0],
        "modifiedon": ["2025-06-01", "2025-06-02"],
        "createdon": ["2025-01-01", "2025-01-02"],
        "valid_from": ["2025-01-01", "2025-01-01"],
        "valid_to": [None, None],
        # Metadata columns (should be dropped)
        "row_id": [1, 2],
        "json_response": ['{"k":"v"}', '{"k":"v2"}'],
        "sync_time": ["2026-01-09", "2026-01-09"],
        "_organizationid_value": ["org1", "org2"],
        "statuscode": [1, 1],
        "_createdby_value": ["u1", "u2"],
        "_modifiedby_value": ["m1", "m2"],
        "versionnumber": [1, 2],
        "importsequencenumber": [None, None],
        "timezoneruleversionnumber": [None, None],
        "_new_developerid_value": [None, None],
        "overriddencreatedon": [None, None],
        "utcconversiontimezonecode": [None, None],
        "_modifiedonbehalfby_value": [None, None],
        "_createdonbehalfby_value": [None, None],
        # All-null column
        "empty_col": [None, None],
    }
    data.update(overrides)
    return pd.DataFrame(data)


class TestEnrichFromAccounts:
    """Tests for _enrich_from_accounts function."""

    def test_text_country_resolved(self):
        """Country as text name matched via vin_countries.vin_name."""
        df = _make_developers()
        accounts = _make_accounts(address1_country=["France", "France"])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert result["country_name"].iloc[0] == "France"
        assert result["country_name"].iloc[1] == "France"

    def test_numeric_country_code_resolved(self):
        """Numeric country code resolved via vin_countryno."""
        df = _make_developers()
        accounts = _make_accounts(address1_country=["4", "4"])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert result["country_name"].iloc[0] == "France"

    def test_alias_country_resolved(self):
        """Aliased country name (e.g. Türkiye) resolved to canonical name."""
        df = _make_developers()
        accounts = _make_accounts(address1_country=["Türkiye", "Türkiye"])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert result["country_name"].iloc[0] == "Turkey"

    def test_no_account_match(self):
        """Developer with no matching account gets NaN for enriched columns."""
        df = _make_developers(_vin_developer_value=["no-match", "no-match"])
        accounts = _make_accounts()
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert pd.isna(result["org_name"].iloc[0])
        assert pd.isna(result["country_name"].iloc[0])

    def test_null_country(self):
        """NULL country in accounts results in NaN country_name."""
        df = _make_developers()
        accounts = _make_accounts(address1_country=[None, None])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert pd.isna(result["country_name"].iloc[0])

    def test_numeric_org_type_nulled(self):
        """Purely numeric org type (e.g. '5001') is set to None."""
        df = _make_developers()
        accounts = _make_accounts(vin_organisationtype=["5001", "5003"])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert pd.isna(result["vin_organisationtype"].iloc[0])
        assert pd.isna(result["vin_organisationtype"].iloc[1])

    def test_text_org_type_preserved(self):
        """Text org type (e.g. 'For Profit SME') is preserved."""
        df = _make_developers()
        accounts = _make_accounts(
            vin_organisationtype=["For Profit SME", "Academic/Research"]
        )
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert result["vin_organisationtype"].iloc[0] == "For Profit SME"
        assert result["vin_organisationtype"].iloc[1] == "Academic/Research"

    def test_address1_country_column_dropped(self):
        """Raw address1_country column is removed after resolution."""
        df = _make_developers()
        accounts = _make_accounts()
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert "address1_country" not in result.columns
        assert "country_name" in result.columns

    def test_not_specified_country_resolves_to_null(self):
        """'Not specified' in country alias maps to None."""
        df = _make_developers()
        accounts = _make_accounts(address1_country=["Not specified", "Not specified"])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert pd.isna(result["country_name"].iloc[0])

    def test_case_insensitive_country_match(self):
        """Country matching is case-insensitive."""
        df = _make_developers()
        accounts = _make_accounts(address1_country=["france", "FRANCE"])
        countries = _make_countries()

        result = _enrich_from_accounts(df, accounts, countries)
        assert result["country_name"].iloc[0] == "France"
        assert result["country_name"].iloc[1] == "France"


class TestTransformDevelopers:
    """Tests for transform_developers function."""

    def test_drops_metadata_columns(self):
        df = _make_developers()
        result, _ = transform_developers(df)
        for col in [
            "row_id",
            "json_response",
            "sync_time",
            "_organizationid_value",
            "statuscode",
            "_createdby_value",
            "_modifiedby_value",
            "versionnumber",
        ]:
            assert col not in result.columns

    def test_renames_columns(self):
        df = _make_developers()
        result, _ = transform_developers(df)
        assert "developerid" in result.columns
        assert "vin_developerid" not in result.columns
        assert "developer_product_name" in result.columns
        assert "vin_name" not in result.columns
        assert "candidateid" in result.columns
        assert "_vin_cap_value" not in result.columns
        assert "accountid" in result.columns
        assert "_vin_developer_value" not in result.columns

    def test_drops_empty_columns_preserves_valid_to(self):
        df = _make_developers()
        result, _ = transform_developers(df)
        assert "valid_to" in result.columns
        assert "empty_col" not in result.columns

    def test_works_without_lookup_tables(self):
        df = _make_developers()
        result, cleaned = transform_developers(df, lookup_tables=None)
        assert isinstance(result, pd.DataFrame)
        assert len(cleaned) == 0
        # Without enrichment, no org_name or country_name columns
        assert "org_name" not in result.columns

    def test_works_with_lookup_tables(self):
        df = _make_developers()
        lookup = {
            "accounts": _make_accounts(),
            "vin_countries": _make_countries(),
        }
        result, _ = transform_developers(df, lookup_tables=lookup)
        assert "org_name" in result.columns
        assert "country_name" in result.columns
        assert "org_type" in result.columns

    def test_does_not_modify_original(self):
        df = _make_developers()
        original_columns = list(df.columns)
        original_len = len(df)
        transform_developers(df)
        assert list(df.columns) == original_columns
        assert len(df) == original_len

    def test_returns_tuple_with_empty_dict(self):
        df = _make_developers()
        result, cleaned = transform_developers(df)
        assert isinstance(result, pd.DataFrame)
        assert isinstance(cleaned, dict)
        assert len(cleaned) == 0

    def test_org_type_renamed_from_vin_organisationtype(self):
        """vin_organisationtype is renamed to org_type after enrichment."""
        df = _make_developers()
        lookup = {
            "accounts": _make_accounts(
                vin_organisationtype=["For Profit SME", "Academic/Research"]
            ),
            "vin_countries": _make_countries(),
        }
        result, _ = transform_developers(df, lookup_tables=lookup)
        assert "org_type" in result.columns
        assert "vin_organisationtype" not in result.columns
        assert result["org_type"].iloc[0] == "For Profit SME"
