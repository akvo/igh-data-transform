"""Candidates table transformation (vin_candidates)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
    replace_values,
)

# Temporal source columns consumed by expansion
_TEMPORAL_SOURCE_COLS = [
    "new_rdstage2021",
    "new_2023currentrdstage",
    "new_2024currentrdstage",
    "_vin_currentrndstage_value",
]

_COLUMNS_TO_DROP = [
    "row_id",
    "new_potentialforacceleratedorconditionalregulator",
    "crc8b_updatedforipps20",
    "vin_stringentregulatoryauthoritysraapprovalda",
    "vin_includeinevgendatabase",
    "new_platform",
    "new_personslivingwithhiv",
    "vin_id",
    "crc8b_ghtcroireviewstatus",
    "vin_2019candidateidnumber",
    "new_medicated",
    "crc8b_ndpipelinereviewstatus",
    "vin_numberofcountrieswithproductapproval_date",
    "vin_casnumber",
    "new_exportgroup",
    "crc8b_ndpipelinereviewdate",
    "new_pipscomments",
    "new_adjuvantrequirement",
    "new_whreviewdate",
    "_modifiedby_value",
    "vin_reviewpersonaggregated",
    "vin_iggformatanimalderived",
    "new_durationofaction",
    "new_reviewstatus",
    "vin_fdapregnancylabelingpregnancyrisksummary",
    "new_regionofregistration",
    "new_estimateddateofregulatoryfiling",
    "vin_regionspecificityaggregated",
    "vin_sbereviewdate",
    "new_2023includeinevgendatabase",
    "vin_evgenreviewcompleted",
    "timezoneruleversionnumber",
    "new_aim1clinicalusestatus",
    "new_updatedforaim20",
    "new_includeinaim1",
    "_createdby_value",
    "vin_evgenreviewdate",
    "crc8b_includeinipps20",
    "new_thermostabilityandstorage",
    "new_atcclassification",
    "vin_meshheadings",
    "vin_directactionontoxins",
    "vin_duplicateentrycapformorethanonedisease",
    "importsequencenumber",
    "crc8b_srhreviewdate",
    "createdon",
    "statuscode",
    "crc8b_updatedforndpipeline",
    "vin_2019archetype",
    "vin_nationalregulatoryauthorityapprovaldate",
    "new_tppreviewrequired",
    "vin_otherindications",
    "vin_includeinwellcomesbedatabase",
    "new_includeinportal2025",
    "new_aim1archetype",
    "new_reviewdateipps30",
    "new_chimstudyyesno",
    "vin_productiontechniqueandorimmunizationstrat",
    "new_ipps30reviewstatus",
    "vin_reviewnotes",
    "new_profilestatus",
    "vin_adisid",
    "new_mamedicinesubtype",
    "crc8b_includeinghtcroi",
    "new_includeinwhpipeline",
    "vin_2019pcrpipelineinclusion",
    "vin_adisurl",
    "new_ctregistrylink2",
    "_owninguser_value",
    "vin_2019disease",
    "crc8b_platformtechnologyused",
    "new_dateforstream",
    "_vin_pipct_value",
    "new_reviewdate",
    "new_cttitle",
    "new_includeinipps30",
    "vin_ifyesdoesclinicaltrialevidencepredate2015",
    "vin_2019product",
    "new_snakespecies_producttestedin",
    "new_safetyandreactogenicityprofile",
    "new_pesubtype",
    "crc8b_descriptionofipps20update",
    "new_exportorder",
    "new_updatedforipps30",
    "vin_syndromicprofiles",
    "vin_routeofadministrationaggregated",
    "new_safetyreactogenicityprofile",
    "vin_isthereevidencethatthecandidatehasbeentes",
    "_vin_archetype_value",
    "new_clinicaltrialgeographicallocation",
    "vin_2019candidatename",
    "crc8b_srhprofilestatus",
    "new_includeinaim2",
    "vin_2019complexitysecondpass",
    "crc8b_includeinippscdf",
    "new_aim1pcrreviewnotes",
    "vin_inactivedevelopmenttype",
    "new_impactmodellingurl",
    "new_aim1devstatus",
    "new_2024includeinpipeline",
    "vin_countryspecificityaggregated",
    "new_whprofilestatus",
    "new_efficacyandclinicalendpoints",
    "new_aim1identifier",
    "crc8b_originofipps20update",
    "vin_2019subdisease",
    "vin_stringentregulatoryauthoritysraapproval",
    "new_aim1highestrdstage",
    "crc8b_includeinndpipeline",
    "_new_numberofcts_value",
    "vin_2019status",
    "_ownerid_value",
    "crc8b_includeinsrhpipeline",
    "new_whereistheresearchlocated",
    "vin_ifigformatrecombinantotherpleasespecify",
    "crc8b_ipps20reviewstatus",
    "vin_iggformatrecombinant",
    "new_ctregistrylink3",
    "vin_includeinp2imodel",
    "new_includeinpipeline2021",
    "json_response",
    "sync_time",
    "vin_igfinalproducttypepreparationifapplicable",
    "crc8b_includeinipps10",
    "new_aim1currentrdstage",
    "new_treatmentregvxschedule",
    "new_isthisproductspecifictoaregion",
    "_owningbusinessunit_value",
    "new_interactionwithotherpharmacologicalproducts",
    "_vin_clinicalusestatus_value",
    "_vin_currentrndstage_value",
]

_COLUMN_RENAMES = {
    "vin_name": "candidate_name",
    "new_sbereviewstatus": "sbereviewstatus",
    "vin_targettoxinclass": "targettoxinclass",
    "vin_knownfundersaggregated": "knownfundersaggregated",
    "new_vin_whosnakespeciesriskcatagainst": "whosnakespeciesriskcatagainst",
    "vin_approvalstatus": "approvalstatus",
    "vin_venomspecificity": "venomspecificity",
    "new_pressuretype": "pressuretype",
    "new_ctregistrylink": "ctregistrylink",
    "vin_approvedforuseinpregnantorlactatingwomen": "approvedforuseinpregnantorlactatingwomen",
    "new_technologyprinciple": "technologyprinciple",
    "new_ctenddate": "ctenddate",
    "vin_2019developers": "2019developers",
    "vin_usfdaapprovaldate": "usfdaapprovaldate",
    "vin_alternativenames": "alternativenames",
    "vin_product": "product",
    "vin_researchedinpregnantwomenorlactatingwomen": "researchedinpregnantwomenorlactatingwomen",
    "vin_target": "target",
    "vin_emaapprovalstatus": "emaapprovalstatus",
    "vin_inactivedevelopmentreason": "inactivedevelopmentreason",
    "new_2024knownfunders": "2024knownfunders",
    "vin_emaapprovaldate": "emaapprovaldate",
    "vin_previouslyidentifiedcandidate": "previouslyidentifiedcandidate",
    "new_2023knownfundersaggregated": "2023knownfunders",
    "crc8b_srhindication": "WH_indication",
    "vin_developersaggregated": "developersaggregated",
    "vin_japanesemhlwapprovalstatus": "japanesemhlwapprovalstatus",
    "vin_typeofpreclinicalresults": "typeofpreclinicalresults",
    "vin_stringentregulatoryauthorityapproval": "SRA_approvalstatus",
    "vin_whoprequalificationdate": "whoprequalificationdate",
    "vin_technologytype": "technologytype",
    "new_snakespeciesagainst": "snakespeciesagainst",
    "vin_numberofcountrieswithproductapproval": "numberofcountrieswithproductapproval",
    "new_developers2025": "developers2025",
    "vin_indication": "indication",
    "vin_snakespecies": "snakespecies",
    "vin_specifictargettoxinclass": "specifictargettoxinclass",
    "new_ctstartdate": "ctstartdate",
    "vin_chemicalname": "chemicalname",
    "vin_specimentype": "specimentype",
    "new_indicationtype": "indicationtype",
    "vin_mechanismofaction": "mechanismofaction",
    "vin_recentupdates": "recentupdates",
    "new_knownfunders2025": "knownfunders2025",
    "vin_usfdaapprovalstatus": "usfdaapprovalstatus",
    "vin_preclinicalresultsstatus": "preclinicalresultsstatus",
    "new_2023developersaggregated": "2023developersaggregated",
    "new_anticipatedsranraandwhopqstrategy": "anticipatedwhostrategy",
    "vin_whoprequalification": "whoprequalification",
    "new_2024developersaggregated": "2024developersaggregated",
    "vin_healthcarefacilitylevel": "healthcarefacilitylevel",
    "_vin_captype_value": "captype_value",
    "_vin_mainproduct_value": "mainproduct_value",
    "_vin_subproduct_value": "subproduct_value",
    "_vin_secondarydisease_value": "secondarydisease_value",
    "new_snakespeciesproductderivedfrom": "snakespeciesproductderivedfrom",
    "vin_keyfeatureschallenges": "keyfeatureschallenges",
    "new_agespecific": "agespecific",
    "new_testformat": "testformat",
    "vin_candidateid": "candidateid",
    "new_whoimmunizingspecies": "whoimmunizingspecies",
    "vin_japanesemhlwapprovaldate": "japanesemhlwapprovaldate",
    "vin_developmentstatus": "developmentstatus",
    "vin_snakefamily": "snakefamily",
    "vin_otherstringentregulatoryauthoritydate": "otherSRAdate",
    "vin_nationalregulatoryauthorityapprovalstatus": "NRAapprovalstatus",
    "new_2024developmentstatus": "2024developmentstatus",
    "_vin_disease_value": "diseasevalue",
    "new_includeinpipeline": "includeinpipeline",
    "vin_whosnakespeciesriskcategory": "whosnakespeciesriskcategory",
    "vin_preclinicalresultssource": "preclinicalresultssource",
    "vin_countrieswhereproductisapprovedaggregated": "countries_product_approved",
    "new_whoparaspecificityspecies": "whoparaspecificityspecies",
    "vin_thermostability": "thermostability",
    "new_peseverity": "peseverity",
    "new_2023developmentstatus": "2023developmentstatus",
    "_vin_currentrndstage_value": "rdstage_value",
    "vin_approvingauthority": "approvingauthority",
    "vin_2019stagepcr": "2019RDstage",
}

_PRODUCT_TYPE_MAPPING = {
    "Dietary supplement": "Dietary supplements",
    "Diagnostic": "Diagnostics",
    "Drug": "Drugs",
    "Functional foods": "Dietary supplements",
    "Microbial interventions": "Microbial interventions",
    "Chemical vector control products": "VCP",
    "Biological vector control products": "VCP",
    "Vector control products Reservoir targeted vaccines": "VCP",
    "Vector control products": "VCP",
    "Reservoir targeted vaccines": "Vaccines",
}

_RD_STAGE_MAPPING = {
    "Late development (design and development)": "Late development",
    "Late development (clinical validation and launch readiness)": "Late development",
    "Phase III - Drugs": "Phase III",
    "Not applicable": "N/A",
    "Discovery": "Discovery and Preclinical",
    "Early development (concept and research)": "Early development",
    "Early development (feasibility and planning)": "Early development",
    "Late development (clinical validation and launch readiness) - Diagnostics": "Late development",
    "Late development - Diagnostics": "Late development",
    "Phase II - Vaccines": "Phase II",
    "Phase III - Vaccines": "Phase III",
    "Phase IV - Vaccines": "Phase IV",
    "Phase I - Vaccines": "Phase I",
    "Preclinical - Vaccines": "Preclinical",
    "Regulatory filing - Diagnostics": "Regulatory filing",
    "Preclinical - Drugs": "Preclinical",
}

_PRESSURE_TYPE_MAPPING = {
    "Negative pressure ": "Negative pressure",
    "Positive pressure ": "Positive pressure",
    "Not applicable ": "N/A",
}

_APPROVAL_STATUS_CONSOLIDATION = {
    862890001: 909670000,  # Adopted -> Approved
}

_APPROVING_AUTHORITY_CONSOLIDATION = {
    909670002: 909670001,  # SRA Other -> SRA
}

_INDICATION_TYPE_CONSOLIDATION = {
    100000003: 100000001,  # Duplicate Treatment -> Treatment
    100000004: 100000000,  # Duplicate Prevention -> Prevention
    100000005: 100000002,  # Duplicate Prevention & treatment -> Prevention & treatment
}

_PRECLINICAL_RESULTS_CONSOLIDATION = {
    909670004.0: 909670002.0,  # Unavailable/unknown -> Unknown
}

# Option set rows to remove (by code)
_INDICATION_TYPE_CODES_TO_REMOVE = {100000003, 100000004, 100000005}
_PRECLINICAL_RESULTS_CODES_TO_REMOVE = {909670004}
_APPROVAL_STATUS_CODES_TO_REMOVE = {862890001}
_APPROVING_AUTHORITY_CODES_TO_REMOVE = {909670002}


def _resolve_rdstage_fk(
    df: pd.DataFrame,
    rdstageproducts: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve _vin_currentrndstage_value GUIDs to RD stage text names.

    Joins with vin_rdstageproducts and strips the ' - ProductType' suffix
    from vin_name (e.g. 'Phase III - Drugs' -> 'Phase III').
    """
    lookup = rdstageproducts.set_index("vin_rdstageproductid")["vin_name"]
    # Strip product suffix: 'Phase III - Drugs' -> 'Phase III'
    lookup = lookup.str.rsplit(" - ", n=1).str[0]
    df = df.copy()
    df["_resolved_rdstage_2025"] = df["_vin_currentrndstage_value"].map(lookup)
    return df


def _expand_temporal_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create time-versioned rows from year-specific RD stage columns.

    Produces one row per candidate per year where RD stage data exists.
    valid_to = start of candidate's next populated period (None for latest).
    Must be called before column renaming (uses original bronze names).
    """
    year_configs = [
        ("new_rdstage2021", "2021-01-01"),
        ("new_2023currentrdstage", "2023-01-01"),
        ("new_2024currentrdstage", "2024-01-01"),
        ("_resolved_rdstage_2025", "2025-01-01"),
    ]

    frames = []
    for src_col, valid_from in year_configs:
        if src_col not in df.columns:
            continue
        year_df = df[df[src_col].notna()].copy()
        if year_df.empty:
            continue
        year_df["new_currentrdstage"] = year_df[src_col]
        year_df["valid_from"] = valid_from
        frames.append(year_df)

    if not frames:
        df_result = df.copy()
        df_result["new_currentrdstage"] = None
        df_result["valid_from"] = None
        df_result["valid_to"] = None
        cols_to_drop = _TEMPORAL_SOURCE_COLS + ["_resolved_rdstage_2025"]
        return df_result.drop(
            columns=[c for c in cols_to_drop if c in df_result.columns]
        )

    df_expand = pd.concat(frames, ignore_index=True)
    df_expand = df_expand.sort_values(by=["vin_candidateid", "valid_from"])

    # Per-candidate valid_to = next row's valid_from (None for last)
    df_expand["valid_to"] = df_expand.groupby("vin_candidateid")["valid_from"].shift(-1)

    # Drop consumed columns
    cols_to_drop = _TEMPORAL_SOURCE_COLS + ["_resolved_rdstage_2025"]
    df_expand = df_expand.drop(
        columns=[c for c in cols_to_drop if c in df_expand.columns]
    )
    return df_expand


def transform_candidates(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
    lookup_tables: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_candidates table from Bronze to Silver.

    Args:
        df: Raw candidates DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames keyed by table name.
        lookup_tables: Dict of lookup DataFrames (e.g. vin_rdstageproducts).

    Returns:
        Tuple of (transformed DataFrame, dict of cleaned option sets).
    """
    # 1. Resolve FK for 2025 RD stage
    if lookup_tables and "vin_rdstageproducts" in lookup_tables:
        df = _resolve_rdstage_fk(df, lookup_tables["vin_rdstageproducts"])

    # 2. Temporal expansion (reads original bronze column names)
    df = _expand_temporal_rows(df)

    # 3. Drop columns (temporal sources already consumed by expansion)
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)
    df = drop_empty_columns(df, preserve=["valid_to", "valid_from"])

    # 4. Rename columns
    df = rename_columns(df, _COLUMN_RENAMES)

    # 5. Standardize new_currentrdstage (from SCD2 expansion)
    if "new_currentrdstage" in df.columns:
        df = replace_values(df, "new_currentrdstage", _RD_STAGE_MAPPING)
        # Strip remaining " - ProductType" suffixes
        mask = df["new_currentrdstage"].str.contains(" - ", na=False)
        df.loc[mask, "new_currentrdstage"] = (
            df.loc[mask, "new_currentrdstage"].str.split(" - ").str[0]
        )

    # 6. Standardize other categorical values
    if "pressuretype" in df.columns:
        df = replace_values(df, "pressuretype", _PRESSURE_TYPE_MAPPING)
    if "product" in df.columns:
        df = replace_values(df, "product", _PRODUCT_TYPE_MAPPING)

    # Consolidate option set code values
    if "approvalstatus" in df.columns:
        df = replace_values(df, "approvalstatus", _APPROVAL_STATUS_CONSOLIDATION)
    if "approvingauthority" in df.columns:
        df = replace_values(
            df, "approvingauthority", _APPROVING_AUTHORITY_CONSOLIDATION
        )

    # 7. Filter to pipeline-included candidates
    if "includeinpipeline" in df.columns:
        df = df[
            (df["includeinpipeline"] == 100000000)
            | (df["includeinpipeline"] == 100000002)
        ].copy()

    # 8. Post-filter consolidations
    if "indicationtype" in df.columns:
        df = replace_values(df, "indicationtype", _INDICATION_TYPE_CONSOLIDATION)
    if "preclinicalresultsstatus" in df.columns:
        df = replace_values(
            df, "preclinicalresultsstatus", _PRECLINICAL_RESULTS_CONSOLIDATION
        )

    # Clean option sets
    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets:
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_new_indicationtype",
            _INDICATION_TYPE_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_vin_preclinicalresultsstatus",
            _PRECLINICAL_RESULTS_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_vin_approvalstatus",
            _APPROVAL_STATUS_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets,
            cleaned_option_sets,
            "_optionset_vin_approvingauthority",
            _APPROVING_AUTHORITY_CODES_TO_REMOVE,
        )

    return df, cleaned_option_sets


def _dedup_option_set(
    option_sets: dict[str, pd.DataFrame],
    cleaned: dict[str, pd.DataFrame],
    table_name: str,
    codes_to_remove: set,
) -> None:
    """Remove duplicate codes from an option set table."""
    if table_name in option_sets:
        os_df = option_sets[table_name].copy()
        os_df = os_df[~os_df["code"].isin(codes_to_remove)]
        cleaned[table_name] = os_df.reset_index(drop=True)
