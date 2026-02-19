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
    - FK_VIA_CANDIDATE:column: Cross-reference FK via candidate relationship
    - DELIMITED_VALUE: Placeholder for values extracted from delimited field
    - OPTIONSET_LABEL / OPTIONSET_CODE: For dimensions built from optionset cache
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
        "product_level": "CASE WHEN vin_type = 909670000 THEN 'top-level' ELSE 'sub-product' END",
        "parent_product_id": "_vin_relatedproduct_value",
    },
    "dim_candidate_core": {
        "_source_table": "vin_candidates",
        "_pk": "candidate_key",
        "candidateid": "candidateid",
        "candidate_name": "candidate_name",
        "vin_candidate_code": "vin_candidateno",
        "developers_agg": "developersaggregated",
        "alternative_names": "alternativenames",
        "target": "target",
        "mechanism_of_action": "mechanismofaction",
        "key_features": "keyfeatureschallenges",
        "known_funders_agg": "knownfundersaggregated",
        "development_status": "OPTIONSET:developmentstatus",
        "current_rd_stage": "new_currentrdstage",
        "countries_approved_count": "numberofcountrieswithproductapproval",
        "countries_approved_agg": "countries_product_approved",
        "candidate_type": "CASE WHEN captype_value = 'c1746ad3-93d1-f011-bbd3-00224892cefa' THEN 'Candidate' WHEN captype_value = '545d63d9-93d1-f011-bbd3-00224892cefa' THEN 'Product' ELSE 'Other' END",
        "indication": "indication",
    },
    "dim_disease": {
        "_source_table": "vin_diseases",
        "_pk": "disease_key",
        "diseaseid": "diseaseid",
        "disease_name": "name",
        "global_health_area": "OPTIONSET:globalhealtharea",
        "disease_type": "COALESCE(disease_simple, 'Unknown')",
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
            "distinct_cols": ["technology_type"],
        },
        "technology_type": "COALESCE(technologytype, 'Unknown')",
    },
    "dim_candidate_regulatory": {
        "_source_table": "vin_candidates",
        "_pk": "regulatory_key",
        "_special": {
            "distinct": True,
            "distinct_cols": [
                "approval_status",
                "fda_approval_date",
                "who_prequal_date",
                "who_prequalification",
                "nra_approval_status",
            ],
        },
        "approval_status": "OPTIONSET:approvalstatus",
        "fda_approval_date": "usfdaapprovaldate",
        "who_prequal_date": "whoprequalificationdate",
        "who_prequalification": "OPTIONSET:whoprequalification",
        "nra_approval_status": "OPTIONSET:NRAapprovalstatus",
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
        "iso_code": "LOOKUP:COUNTRY_ISO_CODE",
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
        "_special": {"fk_lookups": True},
        "rdpriorityid": "rdpriorityid",
        "priority_name": "name",
        "indication": "COALESCE(indication, '')",
        "intended_use": "COALESCE(intendeduse, '')",
        "disease_key": "FK:dim_disease.diseaseid|diseasevalue",
    },
    "dim_developer": {
        "_source_table": "vin_candidates",
        "_pk": "developer_key",
        "_special": {
            "extract_distinct_from_delimited": True,
            "source_column": "developersaggregated",
            "delimiter": ";",
        },
        "developer_name": "DELIMITED_VALUE",  # Each parsed value becomes a row
    },
    "dim_age_group": {
        "_source_table": None,
        "_pk": "age_group_key",
        "_special": {"from_optionset": True, "optionset_name": "agespecific"},
        "age_group_name": "OPTIONSET_LABEL",
        "option_code": "OPTIONSET_CODE",
    },
    "dim_approving_authority": {
        "_source_table": None,
        "_pk": "authority_key",
        "_special": {"from_optionset": True, "optionset_name": "approvingauthority"},
        "authority_name": "OPTIONSET_LABEL",
        "option_code": "OPTIONSET_CODE",
    },
    "dim_funder": {
        "_source_table": "vin_candidates",
        "_pk": "funder_key",
        "_special": {
            "extract_distinct_from_delimited": True,
            "source_column": "knownfundersaggregated",
            "delimiter": ";",
        },
        "funder_name": "DELIMITED_VALUE",
    },
    # =========================================================================
    # FACT TABLES
    # =========================================================================
    "fact_pipeline_snapshot": {
        "_source_table": "vin_candidates",
        "_pk": "snapshot_id",
        "_special": {"fk_lookups": True},
        # Foreign keys - resolved via lookup during transform
        "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
        "product_key": "FK:dim_product.vin_productid|mainproduct_value",
        "disease_key": "FK:dim_disease.diseaseid|diseasevalue",
        "secondary_disease_key": "FK:dim_disease.diseaseid|secondarydisease_value",
        "sub_product_key": "FK:dim_product.vin_productid|subproduct_value",
        "technology_key": "FK:dim_candidate_tech.COMPOSITE",
        "regulatory_key": "FK:dim_candidate_regulatory.COMPOSITE",
        "phase_key": "FK:dim_phase.phase_name|EXTRACT_PHASE:new_currentrdstage",
        "date_key": "FK:dim_date.full_date|EXTRACT_DATE:valid_from",
        "is_active_flag": "CASE WHEN statecode = 0 THEN 1 ELSE 0 END",
    },
    "fact_clinical_trial_event": {
        "_source_table": "vin_clinicaltrials",
        "_pk": "trial_id",
        "_special": {"fk_lookups": True},
        "clinicaltrialid": "clinicaltrialid",
        "candidate_key": "FK:dim_candidate_core.candidateid|candidate_value",
        "disease_key": "FK_VIA_CANDIDATE:disease_key",
        "product_key": "FK_VIA_CANDIDATE:product_key",
        "start_date_key": "FK:dim_date.full_date|startdate",
        "trial_name": "trialid",
        "trial_title": "title",
        "trial_phase": "ctphase",
        "enrollment_count": "COALESCE(cttenrolment, 0)",
        "status": "OPTIONSET:ctstatus",
        "sponsor": "sponsor",
        "locations": "locations",
        "age_groups": "age",
        "study_type": "studytype",
        "source_text": "vin_source",
    },
    "fact_publication": {
        "_source_table": "vin_sources",
        "_pk": "publication_id",
        "_special": {"fk_lookups": True},
        "candidate_key": "FK:dim_candidate_core.candidateid|_vin_regardingid_value",
        "title": "vin_name",
        "url": "vin_url",
        "description": "vin_description",
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
                    "table": "vin_developers",
                    "candidate_col": "candidateid",
                    "country_col": "country_name",
                    "location_scope": "Developer Location",
                    "country_name_lookup": True,
                },
            ]
        },
        "candidate_key": "FK:dim_candidate_core.candidateid|candidate_col",
        "country_key": "FK:dim_geography.vin_countryid|country_col",
        "location_scope": "LITERAL:location_scope",
    },
    "bridge_candidate_developer": {
        "_source_table": "vin_candidates",
        "_pk": None,
        "_special": {
            "bridge_from_delimited": True,
            "source_column": "developersaggregated",
            "delimiter": ";",
            "dimension_table": "dim_developer",
            "dimension_lookup_col": "developer_name",
        },
        "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
        "developer_key": "FK:dim_developer.developer_name|DELIMITED_VALUE",
    },
    "bridge_candidate_priority": {
        "_source_table": "vin_vin_candidate_vin_rdpriorityset",
        "_pk": None,
        "candidate_key": "FK:dim_candidate_core.candidateid|vin_candidateid",
        "priority_key": "FK:dim_priority.rdpriorityid|vin_rdpriorityid",
    },
    "bridge_candidate_age_group": {
        "_source_table": "_junction_vin_candidates_new_agespecific",
        "_pk": None,
        "candidate_key": "FK:dim_candidate_core.candidateid|entity_id",
        "age_group_key": "FK:dim_age_group.option_code|option_code",
    },
    "bridge_candidate_approving_authority": {
        "_source_table": "_junction_vin_candidates_vin_approvingauthority",
        "_pk": None,
        "candidate_key": "FK:dim_candidate_core.candidateid|entity_id",
        "authority_key": "FK:dim_approving_authority.option_code|option_code",
    },
    "bridge_candidate_organization": {
        "_source_table": "vin_vin_candidate_accountset",
        "_pk": None,
        "candidate_key": "FK:dim_candidate_core.candidateid|vin_candidateid",
        "organization_key": "FK:dim_organization.accountid|accountid",
    },
    "bridge_candidate_funder": {
        "_source_table": "vin_candidates",
        "_pk": None,
        "_special": {
            "bridge_from_delimited": True,
            "source_column": "knownfundersaggregated",
            "delimiter": ";",
            "dimension_table": "dim_funder",
            "dimension_lookup_col": "funder_name",
        },
        "candidate_key": "FK:dim_candidate_core.candidateid|candidateid",
        "funder_key": "FK:dim_funder.funder_name|DELIMITED_VALUE",
    },
    "bridge_trial_geography": {
        "_source_table": "vin_vin_clinicaltrial_vin_countryset",
        "_pk": None,
        "_special": {"trial_bridge": True},
        "trial_key": "FK:fact_clinical_trial_event.clinicaltrialid|vin_clinicaltrialid",
        "country_key": "FK:dim_geography.vin_countryid|vin_countryid",
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
    "dim_age_group",
    "dim_approving_authority",
    # Dimensions with special handling
    "dim_candidate_core",
    "dim_candidate_tech",
    "dim_candidate_regulatory",
    "dim_developer",  # Extracted from vin_candidates.vin_developersaggregated
    "dim_funder",  # Extracted from vin_candidates.vin_knownfundersaggregated
    # Facts (depend on dimensions)
    "fact_pipeline_snapshot",
    "fact_clinical_trial_event",
    "fact_publication",
    # Bridges (depend on dimensions and facts)
    "bridge_candidate_geography",
    "bridge_candidate_developer",
    "bridge_candidate_priority",
    "bridge_candidate_age_group",
    "bridge_candidate_approving_authority",
    "bridge_candidate_organization",
    "bridge_candidate_funder",
    "bridge_trial_geography",
]
