"""Tests for silver_to_gold country ISO code lookups."""

from igh_data_transform.transformations.silver_to_gold.config.country_iso_codes import (
    COUNTRY_ISO_CODES,
    _MANUAL_OVERRIDES,
    _build_iso_lookup,
    lookup_iso_code,
)


class TestBuildIsoLookup:
    def test_includes_pycountry_entries(self):
        lookup = _build_iso_lookup()
        assert lookup["France"] == "FRA"
        assert lookup["Germany"] == "DEU"

    def test_includes_manual_overrides(self):
        lookup = _build_iso_lookup()
        for name, code in _MANUAL_OVERRIDES.items():
            assert lookup[name] == code

    def test_overrides_take_priority(self):
        """Manual overrides should override pycountry entries."""
        lookup = _build_iso_lookup()
        # Turkey is in both pycountry (as Türkiye) and manual overrides
        assert lookup["Turkey"] == "TUR"


class TestLookupIsoCode:
    def test_standard_country(self):
        assert lookup_iso_code("France") == "FRA"

    def test_manual_override(self):
        assert lookup_iso_code("Cape Verde") == "CPV"
        assert lookup_iso_code("Kosovo") == "XKX"

    def test_non_country_entity_returns_none(self):
        assert lookup_iso_code("African Union") is None
        assert lookup_iso_code("European Union") is None

    def test_empty_string(self):
        assert lookup_iso_code("") is None

    def test_fuzzy_fallback(self):
        # pycountry uses "Korea, Republic of" but fuzzy should resolve "South Korea"
        result = lookup_iso_code("South Korea")
        assert result == "KOR"


class TestCountryIsoCodesDict:
    def test_prebuilt_dict_is_populated(self):
        assert len(COUNTRY_ISO_CODES) > 200

    def test_contains_standard_entries(self):
        assert COUNTRY_ISO_CODES["France"] == "FRA"
