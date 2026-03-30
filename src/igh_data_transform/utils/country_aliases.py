"""
Shared country name aliases for ETL transformations.

Maps variant country names found in source data to canonical names
matching dim_geography (vin_countries.vin_name).
"""

COUNTRY_ALIASES: dict[str, str | None] = {
    # From accounts.address1_country (developer location)
    "Türkiye": "Turkey",
    "United States": "United States of America",
    "Korea": "South Korea",
    "Korea, Republic of": "South Korea",
    "The Netherlands": "Netherlands",
    "United Kindgom": "United Kingdom",  # typo in source data
    "Macedonia": "North Macedonia",
    "Northern Ireland": "United Kingdom",
    "Not specified": None,
    # Trial-location-specific aliases
    "USA": "United States of America",
    "UK": "United Kingdom",
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Iran (Islamic Republic of)": "Iran",
    "Iran, Islamic Republic of": "Iran",
    "Russian Federation": "Russia",
    "Russia Federation": "Russia",
    "Viet Nam": "Vietnam",
    "Lao People's Democratic Republic": "Laos",
    "Eqypt": "Egypt",  # typo in source data
    "Columbia": "Colombia",
    "Czechia": "Czech Republic",
    "Democratic Republic of the Congo": "Democratic Republic of Congo",
    "Congo, Democratic Republic": "Democratic Republic of Congo",
    "Cote d'Ivoire": "Cote D`Ivoire (Ivory Coast)",
    "Côte D'Ivoire": "Cote D`Ivoire (Ivory Coast)",
    "Moldova, Republic of": "Moldova",
    "Unknown": None,
    "unknown": None,
    "N/A": None,
    "United States of America (USA)": "United States of America",
}
