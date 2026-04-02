"""
ISO 3166-1 alpha-3 code lookup for country names.

Uses pycountry for standard lookups with manual overrides for
non-standard names in the Dataverse source data.

Country names are matched by their vin_name from vin_countries.
"""

import pycountry

# Manual overrides for names that pycountry can't resolve or resolves incorrectly.
# Includes non-standard spellings, territories, and non-country entities.
_MANUAL_OVERRIDES: dict[str, str | None] = {
    # Non-country entities (no ISO code)
    "African Union": None,
    "European Union": None,
    "Test_Country": None,
    # Non-standard spellings / alternate names
    "Cape Verde": "CPV",
    "Cote D`Ivoire (Ivory Coast)": "CIV",
    "Democratic Republic of Congo": "COD",
    "Eswatini (Swaziland)": "SWZ",
    "Kosovo": "XKX",  # User-assigned code (not in ISO 3166-1)
    "Macau": "MAC",
    "Occupied Palestinian Territory": "PSE",
    "Saint Kitts & Nevis": "KNA",
    "Timor-Leste (East Timor)": "TLS",
    "Turkey": "TUR",
    "US Virgin Islands": "VIR",
    "West Bank and Gaza": "PSE",
}


def _build_iso_lookup() -> dict[str, str | None]:
    """Build a complete country_name -> ISO alpha-3 lookup using pycountry.

    Returns:
        Dict mapping every known country name to its alpha-3 code (or None).
    """
    lookup: dict[str, str | None] = {}

    # pycountry covers all ISO 3166-1 countries
    for country in pycountry.countries:
        lookup[country.name] = country.alpha_3

    # Override with manual mappings (these take priority)
    lookup.update(_MANUAL_OVERRIDES)

    return lookup


# Pre-built lookup used at import time
COUNTRY_ISO_CODES: dict[str, str | None] = _build_iso_lookup()


def lookup_iso_code(country_name: str) -> str | None:
    """Look up ISO 3166-1 alpha-3 code for a country name.

    Tries exact match first, then pycountry fuzzy search as fallback.

    Args:
        country_name: Country name from vin_countries.vin_name

    Returns:
        ISO alpha-3 code or None if not resolvable
    """
    if not country_name:
        return None

    # Exact match (covers pycountry standard names + manual overrides)
    result = COUNTRY_ISO_CODES.get(country_name)
    if result is not None:
        return result

    # Check if explicitly mapped to None (non-country entities)
    if country_name in COUNTRY_ISO_CODES:
        return None

    # Fuzzy fallback for names not yet seen
    try:
        matches = pycountry.countries.search_fuzzy(country_name)
        if matches:
            code = matches[0].alpha_3
            COUNTRY_ISO_CODES[country_name] = code  # Cache for next lookup
            return code
    except LookupError:
        pass

    return None
