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
    "new_personslivingwithhiv",
    "vin_id",
    "crc8b_ghtcroireviewstatus",
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
    "vin_nationalregulatoryauthorityapprovaldate",
    "new_tppreviewrequired",
    "vin_otherindications",
    "vin_includeinwellcomesbedatabase",
    "new_aim1archetype",
    "new_reviewdateipps30",
    "vin_productiontechniqueandorimmunizationstrat",
    "new_ipps30reviewstatus",
    "vin_reviewnotes",
    "new_profilestatus",
    "vin_adisid",
    "new_mamedicinesubtype",
    "crc8b_includeinghtcroi",
    "new_includeinwhpipeline",
    "vin_adisurl",
    "new_ctregistrylink2",
    "_owninguser_value",
    "crc8b_platformtechnologyused",
    "new_dateforstream",
    "_vin_pipct_value",
    "new_reviewdate",
    "new_cttitle",
    "new_includeinipps30",
    "new_snakespecies_producttestedin",
    "new_safetyandreactogenicityprofile",
    "new_pesubtype",
    "crc8b_descriptionofipps20update",
    "new_exportorder",
    "new_updatedforipps30",
    "vin_syndromicprofiles",
    "new_safetyreactogenicityprofile",
    "vin_isthereevidencethatthecandidatehasbeentes",
    "_vin_archetype_value",
    "new_clinicaltrialgeographicallocation",
    "crc8b_srhprofilestatus",
    "new_includeinaim2",
    "crc8b_includeinippscdf",
    "new_aim1pcrreviewnotes",
    "vin_inactivedevelopmenttype",
    "new_impactmodellingurl",
    "new_aim1devstatus",
    "vin_countryspecificityaggregated",
    "new_whprofilestatus",
    "new_efficacyandclinicalendpoints",
    "new_aim1identifier",
    "crc8b_originofipps20update",
    "vin_stringentregulatoryauthoritysraapproval",
    "new_aim1highestrdstage",
    "crc8b_includeinndpipeline",
    "_new_numberofcts_value",
    "_ownerid_value",
    "crc8b_includeinsrhpipeline",
    "new_whereistheresearchlocated",
    "vin_ifigformatrecombinantotherpleasespecify",
    "crc8b_ipps20reviewstatus",
    "vin_iggformatrecombinant",
    "new_ctregistrylink3",
    "vin_includeinp2imodel",
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
    "new_reviewdatemaster",
    # Backfill-created base columns not needed downstream
    "vin_candidateidnumber",
    "vin_stagepcr",
    "new_rdstage",
    "vin_archetype",
    "new_includeinportal",
    "vin_pcrpipelineinclusion",
    "vin_notes",
    "vin_disease",
    "vin_ifyesdoesclinicaltrialevidencepredate",
    "vin_candidatename",
    "vin_complexitysecondpass",
    "vin_subdisease",
    "vin_status",
    "new_typeofupdate",
    "new_includeinevgendatabase",
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
    # Backfill-created base columns
    "vin_developers": "2019developers",
    "new_knownfunders": "knownfunders",
    "new_knownfundersaggregated": "knownfundersaggregated_temporal",
    "new_developers": "developers",
    "new_developersaggregated": "developersaggregated_temporal",
    "new_currentrdstage": "currentrdstage",
    "new_includeinpipeline": "includeinpipeline",
    "new_developmentstatus": "developmentstatus_temporal",
    "vin_usfdaapprovaldate": "usfdaapprovaldate",
    "vin_alternativenames": "alternativenames",
    "vin_product": "product",
    "vin_researchedinpregnantwomenorlactatingwomen": "researchedinpregnantwomenorlactatingwomen",
    "vin_target": "target",
    "vin_emaapprovalstatus": "emaapprovalstatus",
    "vin_inactivedevelopmentreason": "inactivedevelopmentreason",
    "vin_emaapprovaldate": "emaapprovaldate",
    "vin_previouslyidentifiedcandidate": "previouslyidentifiedcandidate",
    "crc8b_srhindication": "WH_indication",
    "vin_developersaggregated": "developersaggregated",
    "vin_japanesemhlwapprovalstatus": "japanesemhlwapprovalstatus",
    "vin_typeofpreclinicalresults": "typeofpreclinicalresults",
    "vin_stringentregulatoryauthorityapproval": "SRA_approvalstatus",
    "vin_whoprequalificationdate": "whoprequalificationdate",
    "vin_technologytype": "technologytype",
    "new_snakespeciesagainst": "snakespeciesagainst",
    "vin_numberofcountrieswithproductapproval": "numberofcountrieswithproductapproval",
    "vin_indication": "indication",
    "vin_snakespecies": "snakespecies",
    "vin_specifictargettoxinclass": "specifictargettoxinclass",
    "new_ctstartdate": "ctstartdate",
    "vin_chemicalname": "chemicalname",
    "vin_specimentype": "specimentype",
    "new_indicationtype": "indicationtype",
    "vin_mechanismofaction": "mechanismofaction",
    "vin_recentupdates": "recentupdates",
    "vin_usfdaapprovalstatus": "usfdaapprovalstatus",
    "vin_preclinicalresultsstatus": "preclinicalresultsstatus",
    "new_anticipatedsranraandwhopqstrategy": "anticipatedwhostrategy",
    "vin_whoprequalification": "whoprequalification",
    "vin_healthcarefacilitylevel": "healthcarefacilitylevel",
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
    "vin_otherstringentregulatoryauthoritydate": "otherSRAdate",
    "vin_nationalregulatoryauthorityapprovalstatus": "NRAapprovalstatus",
    "_vin_disease_value": "diseasevalue",
    "vin_whosnakespeciesriskcategory": "whosnakespeciesriskcategory",
    "vin_preclinicalresultssource": "preclinicalresultssource",
    "vin_countrieswhereproductisapprovedaggregated": "countries_product_approved",
    "new_whoparaspecificityspecies": "whoparaspecificityspecies",
    "vin_thermostability": "thermostability",
    "new_peseverity": "peseverity",
    "_vin_currentrndstage_value": "rdstage_value",
    "_vin_captype_value": "captype_value",
    "new_testformat": "testformat",
    "vin_routeofadministrationaggregated": "routeofadministration",
    "new_platform": "platform",
    "new_chimstudyyesno": "chimstudyyesno",
    "vin_approvingauthority": "approvingauthority",
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
    # Normalize R&D stage names in the backfill-consolidated currentrdstage
    if "currentrdstage" in df.columns:
        df = replace_values(df, "currentrdstage", _RD_STAGE_MAPPING)

    # Consolidate option set code values
    if "approvalstatus" in df.columns:
        df = replace_values(df, "approvalstatus", _APPROVAL_STATUS_CONSOLIDATION)
    if "approvingauthority" in df.columns:
        df = replace_values(df, "approvingauthority", _APPROVING_AUTHORITY_CONSOLIDATION)

    # No candidate filtering — all candidates are included; the original ETL
    # did not filter by includeinpipeline. Pipeline inclusion is tracked as a
    # dimension attribute, not used for row exclusion.

    # No temporal expansion — the backfill engine already creates proper SCD2
    # versions with valid_from/valid_to across all reporting years.

    # Consolidate indication type and preclinical results
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
