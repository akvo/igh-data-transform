"""Candidates table transformation (vin_candidates)."""

import pandas as pd

from igh_data_transform.transformations.cleanup import (
    drop_columns_by_name,
    drop_empty_columns,
    rename_columns,
    replace_values,
)

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
    "_vin_captype_value",
    "new_regionofregistration",
    "new_estimateddateofregulatoryfiling",
    "vin_regionspecificityaggregated",
    "vin_sbereviewdate",
    "new_2023includeinevgendatabase",
    "vin_currentrdstage",
    "vin_evgenreviewcompleted",
    "timezoneruleversionnumber",
    "new_testformat",
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
    "new_2023currentrdstage": "2023currentrdstage",
    "_vin_mainproduct_value": "mainproduct_value",
    "_vin_subproduct_value": "subproduct_value",
    "_vin_secondarydisease_value": "secondarydisease_value",
    "new_snakespeciesproductderivedfrom": "snakespeciesproductderivedfrom",
    "vin_keyfeatureschallenges": "keyfeatureschallenges",
    "new_agespecific": "agespecific",
    "vin_candidateid": "candidateid",
    "new_whoimmunizingspecies": "whoimmunizingspecies",
    "vin_japanesemhlwapprovaldate": "japanesemhlwapprovaldate",
    "vin_developmentstatus": "developmentstatus",
    "vin_snakefamily": "snakefamily",
    "new_2024currentrdstage": "2024rdstage",
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


def _expand_temporal_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create time-versioned rows for SCD2 tracking.

    For each candidate, creates up to 3 rows (2023, 2024, 2025) based on
    which year columns have non-null RD stage values.

    Args:
        df: Filtered candidates DataFrame with renamed columns.

    Returns:
        Expanded DataFrame with RD_stage, valid_from, valid_to columns.
    """
    frames = []

    # 2023 data
    df_2023 = df[df["2023currentrdstage"].notna()].copy()
    if not df_2023.empty:
        df_2023["RD_stage"] = df_2023["2023currentrdstage"]
        df_2023["valid_from"] = "2023-01-01"
        df_2023["valid_to"] = "2023-12-31"
        frames.append(df_2023)

    # 2024 data
    df_2024 = df[df["2024rdstage"].notna()].copy()
    if not df_2024.empty:
        df_2024["RD_stage"] = df_2024["2024rdstage"]
        df_2024["valid_from"] = "2024-01-01"
        df_2024["valid_to"] = "2024-12-31"
        frames.append(df_2024)

    # Most recent (2025)
    df_mr = df[df["rdstage_value"].notna()].copy()
    if not df_mr.empty:
        df_mr["RD_stage"] = df_mr["rdstage_value"]
        df_mr["valid_from"] = "2025-01-01"
        df_mr["valid_to"] = "2025-12-31"
        frames.append(df_mr)

    if not frames:
        df_result = df.copy()
        df_result["RD_stage"] = None
        return df_result

    df_expand = pd.concat(frames, ignore_index=True)
    df_expand = df_expand.sort_values(by=["candidate_name"])
    return df_expand


def transform_candidates(
    df: pd.DataFrame,
    option_sets: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Transform vin_candidates table from Bronze to Silver.

    Args:
        df: Raw candidates DataFrame from Bronze layer.
        option_sets: Dict of option set DataFrames keyed by table name.

    Returns:
        Tuple of (transformed DataFrame, dict of cleaned option sets).
    """
    # Drop named metadata columns
    df = drop_columns_by_name(df, _COLUMNS_TO_DROP)

    # Drop all-null columns (preserving valid_to, valid_from)
    df = drop_empty_columns(df, preserve=["valid_to", "valid_from"])

    # Rename columns
    df = rename_columns(df, _COLUMN_RENAMES)

    # Standardize categorical values
    if "pressuretype" in df.columns:
        df = replace_values(df, "pressuretype", _PRESSURE_TYPE_MAPPING)
    if "product" in df.columns:
        df = replace_values(df, "product", _PRODUCT_TYPE_MAPPING)
    if "2024rdstage" in df.columns:
        df = replace_values(df, "2024rdstage", _RD_STAGE_MAPPING)

    # Consolidate option set code values
    if "approvalstatus" in df.columns:
        df = replace_values(df, "approvalstatus", _APPROVAL_STATUS_CONSOLIDATION)
    if "approvingauthority" in df.columns:
        df = replace_values(df, "approvingauthority", _APPROVING_AUTHORITY_CONSOLIDATION)

    # Filter to pipeline-included candidates
    if "includeinpipeline" in df.columns:
        df = df[
            (df["includeinpipeline"] == 100000000)
            | (df["includeinpipeline"] == 100000002)
        ].copy()

    # Temporal expansion
    df = _expand_temporal_rows(df)

    # Consolidate indication type and preclinical results (after expansion)
    if "indicationtype" in df.columns:
        df = replace_values(df, "indicationtype", _INDICATION_TYPE_CONSOLIDATION)
    if "preclinicalresultsstatus" in df.columns:
        df = replace_values(df, "preclinicalresultsstatus", _PRECLINICAL_RESULTS_CONSOLIDATION)

    # Clean option sets
    cleaned_option_sets: dict[str, pd.DataFrame] = {}

    if option_sets:
        _dedup_option_set(
            option_sets, cleaned_option_sets,
            "_optionset_new_indicationtype", _INDICATION_TYPE_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets, cleaned_option_sets,
            "_optionset_vin_preclinicalresultsstatus", _PRECLINICAL_RESULTS_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets, cleaned_option_sets,
            "_optionset_vin_approvalstatus", _APPROVAL_STATUS_CODES_TO_REMOVE,
        )
        _dedup_option_set(
            option_sets, cleaned_option_sets,
            "_optionset_vin_approvingauthority", _APPROVING_AUTHORITY_CODES_TO_REMOVE,
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
