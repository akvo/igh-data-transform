"""
Declarative mapping from Dataverse raw schema to OLAP star schema.

This file serves as both documentation and configuration - the "Rosetta Stone"
for tracing any target column back to its source.

Format:
    "target_table": {
        "_source_table": "source_table_name",
        "_pk": "primary_key_column",
        "_special": {optional special handling instructions},
        "target_column": "source_expression",
    }

Source expressions can be:
    - Simple column: "vin_name"
    - COALESCE: "COALESCE(col, 'default')"
    - CASE WHEN: "CASE WHEN statecode = 0 THEN 1 ELSE 0 END"
    - OPTIONSET:column_name: Resolve integer code to label via optionset lookup
    - FK:target_table.column: Foreign key lookup to get surrogate key
    - DELIMITED_VALUE: Placeholder for values extracted from delimited field
"""

STAR_SCHEMA_MAP = {
    # =========================================================================
    # DIMENSION TABLES
    # =========================================================================
    "dim_product": {
        "_source_table": "vin_products",
        "_pk": "product_key",
        "vin_productid": "vin_productid",
        "product_name": "vin_name",
        "product_type": "vin_name",  # vin_name contains the product type (Vaccines, Drugs, Diagnostics, etc.)
    },
    "dim_candidate_core": {
        "_source_table": "vin_candidates",
        "_pk": "candidate_key",
        "vin_candidateid": "vin_candidateid",
        "candidate_name": "vin_name",
        "vin_candidate_code": "vin_candidateno",
        "developers_agg": "vin_developersaggregated",
    },
    "dim_disease": {
        "_source_table": "vin_diseases",
        "_pk": "disease_key",
        "vin_diseaseid": "vin_diseaseid",
        "disease_name": "vin_name",
        "global_health_area": "OPTIONSET:new_globalhealtharea",
        "disease_type": "COALESCE(new_disease_simple, 'Unknown')",
    },
    "dim_phase": {
        "_source_table": "vin_rdstages",
        "_pk": "phase_key",
        "_special": {"sort_order": "LOOKUP:PHASE_SORT_ORDER"},
        "vin_rdstageid": "vin_rdstageid",
        "phase_name": "vin_name",
        "sort_order": "LOOKUP:PHASE_SORT_ORDER",
    },
    "dim_candidate_tech": {
        "_source_table": "vin_candidates",
        "_pk": "technology_key",
        "_special": {
            "distinct": True,
            "distinct_cols": ["platform", "technology_type", "molecule_type", "route_of_admin"],
        },
        "platform": "COALESCE(new_platform, 'Unknown')",
        "technology_type": "COALESCE(vin_technologytype, 'Unknown')",
        "molecule_type": "OPTIONSET:vin_iggformatanimalderived",
        "route_of_admin": "COALESCE(vin_routeofadministrationaggregated, 'Unknown')",
    },
    "dim_candidate_regulatory": {
        "_source_table": "vin_candidates",
        "_pk": "regulatory_key",
        "_special": {
            "distinct": True,
            "distinct_cols": ["approval_status", "sra_approval_flag", "fda_approval_date", "who_prequal_date"],
        },
        "approval_status": "OPTIONSET:vin_approvalstatus",
        "sra_approval_flag": "OPTIONSET:vin_stringentregulatoryauthoritysraapproval",
        "fda_approval_date": "vin_usfdaapprovaldate",
        "who_prequal_date": "vin_whoprequalificationdate",
    },
    "dim_date": {
        "_source_table": None,  # Generated programmatically
        "_pk": "date_key",
        "_special": {"generate": True, "start_year": 2015, "end_year": 2030},
        "full_date": "GENERATED",
        "year": "GENERATED",
        "quarter": "GENERATED",
    },
    "dim_geography": {
        "_source_table": "vin_countries",
        "_pk": "country_key",
        "vin_countryid": "vin_countryid",
        "country_name": "vin_name",
        "iso_code": "COALESCE(vin_countryno, '')",  # Will need manual mapping if needed
        "region_name": "COALESCE(NULL, 'Unknown')",  # No region in source, placeholder
    },
    "dim_organization": {
        "_source_table": "accounts",
        "_pk": "organization_key",
        "accountid": "accountid",
        "org_name": "name",
        "org_type": "COALESCE(vin_organisationtype, 'Unknown')",
    },
    "dim_priority": {
        "_source_table": "vin_rdpriorities",
        "_pk": "priority_key",
        "vin_rdpriorityid": "vin_rdpriorityid",
        "priority_name": "vin_name",
        "indication": "COALESCE(new_indication, '')",
        "intended_use": "COALESCE(new_intendeduse, '')",
    },
    "dim_developer": {
        "_source_table": "vin_candidates",
        "_pk": "developer_key",
        "_special": {
            "extract_distinct_from_delimited": True,
            "source_column": "vin_developersaggregated",
            "delimiter": ";",
        },
        "developer_name": "DELIMITED_VALUE",  # Each parsed value becomes a row
    },
    # =========================================================================
    # FACT TABLES
    # =========================================================================
    "fact_pipeline_snapshot": {
        "_source_table": "vin_candidates",
        "_pk": "snapshot_id",
        "_special": {"fk_lookups": True},
        # Foreign keys - resolved via lookup during transform
        "candidate_key": "FK:dim_candidate_core.vin_candidateid|vin_candidateid",
        "product_key": "FK:dim_product.vin_productid|_vin_mainproduct_value",
        "disease_key": "FK:dim_disease.vin_diseaseid|_vin_disease_value",
        "technology_key": "FK:dim_candidate_tech.COMPOSITE|new_platform,vin_technologytype,vin_iggformatanimalderived,vin_routeofadministrationaggregated",
        "regulatory_key": "FK:dim_candidate_regulatory.COMPOSITE|vin_approvalstatus,vin_stringentregulatoryauthoritysraapproval,vin_usfdaapprovaldate,vin_whoprequalificationdate",
        "phase_key": "FK:dim_phase.phase_name|EXTRACT_PHASE:new_rdstage",  # Look up by phase_name after extracting from "Phase I - Drugs"
        "date_key": "FK:dim_date.full_date|EXTRACT_DATE:valid_from",  # Snapshot date from SCD2
        "is_active_flag": "CASE WHEN statecode = 0 THEN 1 ELSE 0 END",
    },
    "fact_clinical_trial_event": {
        "_source_table": "vin_clinicaltrials",
        "_pk": "trial_id",
        "_special": {"fk_lookups": True},
        "candidate_key": "FK:dim_candidate_core.vin_candidateid|_vin_candidate_value",
        "disease_key": None,  # Clinical trials don't have direct disease - inherit from candidate
        "start_date_key": "FK:dim_date.full_date|vin_startdate",
        "trial_phase": "vin_ctphase",
        "enrollment_count": "COALESCE(vin_ctenrolment, 0)",
        "status": "OPTIONSET:vin_ctstatus",
    },
    # =========================================================================
    # BRIDGE TABLES
    # =========================================================================
    "bridge_candidate_geography": {
        "_source_table": "UNION",  # Special: combines multiple sources
        "_pk": None,  # No single PK, composite
        "_special": {
            "union_sources": [
                {
                    "table": "_junction_vin_candidates_new_clinicaltrialgeographicallocation",
                    "candidate_col": "entity_id",
                    "country_col": "option_code",  # This is an optionset code, not country ID
                    "location_scope": "Trial Location",
                    "optionset_lookup": "new_clinicaltrialgeographicallocation",
                },
                {
                    "table": "_junction_vin_candidates_new_targetcountry",
                    "candidate_col": "entity_id",
                    "country_col": "option_code",
                    "location_scope": "Target Country",
                    "optionset_lookup": "new_targetcountry",
                },
                {
                    "table": "_junction_vin_candidates_new_whereistheresearchlocated",
                    "candidate_col": "entity_id",
                    "country_col": "option_code",
                    "location_scope": "Developer Location",  # Using research location as developer location
                    "optionset_lookup": "new_whereistheresearchlocated",
                },
            ]
        },
        "candidate_key": "FK:dim_candidate_core.vin_candidateid|candidate_col",
        "country_key": "FK:dim_geography.vin_countryid|country_col",
        "location_scope": "LITERAL:location_scope",
    },
    "bridge_candidate_developer": {
        "_source_table": "vin_candidates",
        "_pk": None,
        "_special": {
            "bridge_from_delimited": True,
            "source_column": "vin_developersaggregated",
            "delimiter": ";",
            "dimension_table": "dim_developer",
            "dimension_lookup_col": "developer_name",
        },
        "candidate_key": "FK:dim_candidate_core.vin_candidateid|vin_candidateid",
        "developer_key": "FK:dim_developer.developer_name|DELIMITED_VALUE",
    },
    "bridge_candidate_priority": {
        "_source_table": "vin_vin_candidate_vin_rdpriorityset",
        "_pk": None,
        "candidate_key": "FK:dim_candidate_core.vin_candidateid|vin_candidateid",
        "priority_key": "FK:dim_priority.vin_rdpriorityid|vin_rdpriorityid",
    },
}

# Table loading order (dimensions first, then facts, then bridges)
TABLE_LOAD_ORDER = [
    # Dimensions (no FK dependencies)
    "dim_product",
    "dim_disease",
    "dim_phase",
    "dim_geography",
    "dim_organization",
    "dim_priority",
    "dim_date",
    # Dimensions with special handling
    "dim_candidate_core",
    "dim_candidate_tech",
    "dim_candidate_regulatory",
    "dim_developer",  # Extracted from vin_candidates.vin_developersaggregated
    # Facts (depend on dimensions)
    "fact_pipeline_snapshot",
    "fact_clinical_trial_event",
    # Bridges (depend on dimensions)
    "bridge_candidate_geography",
    "bridge_candidate_developer",
    "bridge_candidate_priority",
]
