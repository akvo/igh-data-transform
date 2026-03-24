# Session Notes

## Known remaining gaps after temporal backfill integration

- `bridge_candidate_geography` has 159 rows vs 7,795 in reference. The 7,636 missing rows are developer-location geography entries. The `vin_developers` table lacks a `country_name` column in the current Dataverse schema, so this cannot be fixed without a schema change upstream.

- `dim_phase` has 17 rows vs 12 in reference. The current DB has more phases (e.g., "Preclinical", "Phase IV", "Regulatory filing", "Post-marketing surveillance", "Development") because the data snapshot includes candidates referencing them. The reference had fewer distinct phases.

- The temporal_columns_report.json is bundled as a static config file. If the Dataverse schema adds new temporal columns in the future, the report needs manual updating.
