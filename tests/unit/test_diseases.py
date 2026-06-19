"""Tests for diseases transformation."""

import pandas as pd

from igh_data_transform.transformations.diseases import transform_diseases


class TestTransformDiseases:
    """Tests for transform_diseases function."""

    def _make_input_df(self, overrides=None):
        """Create a minimal input DataFrame mimicking vin_diseases."""
        data = {
            "row_id": [1, 2],
            "vin_disease": ["Malaria", "HIV"],
            "createdon": ["2025-01-01", "2025-01-02"],
            "modifiedon": ["2025-06-01", "2025-06-02"],
            "_organizationid_value": ["org-1", "org-1"],
            "crc8b_addedclinicalvalue": [None, None],
            "crc8b_tppppc": [0.0, 0.0],
            "crc8b_addedclinicalvaluedescription": ["desc", None],
            "crc8b_p2iproductlaunch": [0.0, 0.0],
            "versionnumber": [100, 200],
            "statuscode": [1, 1],
            "vin_name": ["Disease A", "Disease B"],
            "statecode": [0, 0],
            "crc8b_realisticlaunch": [None, None],
            "vin_type": [1.0, 2.0],
            "new_secondary_diseae_choice_text": [None, "secondary"],
            "_createdby_value": ["user-1", "user-2"],
            "new_globalhealthareaportal": ["portal-1", None],
            "vin_diseasecode": ["D001", "D002"],
            "_vin_product_value": ["prod-1", "prod-2"],
            "new_disease_simple": ["simple-1", None],
            "importsequencenumber": [None, None],
            "new_incl_eid": [1.0, 0.0],
            "new_diseasefilter": ["filter-1", None],
            "new_disease_sort": ["sort-1", "sort-2"],
            "new_secondary_disease_filter": [40.0, None],
            "new_disease_choice_text": ["choice-1", None],
            "_modifiedby_value": ["mod-1", "mod-2"],
            "vin_diseaseid": ["did-1", "did-2"],
            "_vin_maindisease_value": [None, "main-1"],
            "new_incl_nd": [1.0, 0.0],
            "new_globalhealtharea": [100000000, 100000002],
            "json_response": ['{"k":"v"}', '{"k":"v2"}'],
            "sync_time": ["2026-01-09T12:00:00", "2026-01-09T12:00:01"],
            "valid_from": ["2025-01-01", "2025-01-02"],
            "valid_to": [None, None],
            # All-null columns that get dropped by drop_empty_columns
            "_vin_subproduct_value": [None, None],
            "timezoneruleversionnumber": [None, None],
            "_createdonbehalfby_value": [None, None],
            "utcconversiontimezonecode": [None, None],
            "_modifiedonbehalfby_value": [None, None],
            "overriddencreatedon": [None, None],
        }
        if overrides:
            data.update(overrides)
        return pd.DataFrame(data)

    def _make_option_sets(self):
        """Create option set dict with globalhealtharea."""
        return {
            "_optionset_new_globalhealtharea": pd.DataFrame(
                {
                    "code": [100000000, 100000001, 100000002],
                    "label": [
                        "Neglected disease",
                        "Emerging infectious disease",
                        "Sexual & reproductive health",
                    ],
                    "first_seen": ["2026-01-09", "2026-01-09", "2026-01-09"],
                }
            ),
        }

    def test_drops_metadata_columns(self):
        df = self._make_input_df()
        result, _ = transform_diseases(df)
        dropped = [
            "row_id",
            "createdon",
            "modifiedon",
            "_organizationid_value",
            "crc8b_addedclinicalvalue",
            "crc8b_tppppc",
            "crc8b_addedclinicalvaluedescription",
            "crc8b_p2iproductlaunch",
            "statuscode",
            "statecode",
            "_createdby_value",
            "new_globalhealthareaportal",
            "importsequencenumber",
            "new_incl_eid",
            "_modifiedby_value",
            "new_incl_nd",
            "json_response",
            "sync_time",
        ]
        for col in dropped:
            assert col not in result.columns

    def test_drops_empty_columns_preserving_valid_to(self):
        df = self._make_input_df()
        result, _ = transform_diseases(df)
        assert "valid_to" in result.columns
        assert "_vin_subproduct_value" not in result.columns
        assert "_createdonbehalfby_value" not in result.columns

    def test_renames_columns(self):
        df = self._make_input_df()
        result, _ = transform_diseases(df)
        assert "disease" in result.columns
        assert "vin_disease" not in result.columns
        assert "name" in result.columns
        assert "vin_name" not in result.columns
        assert "type" in result.columns
        assert "secondary_disease_name" in result.columns
        assert "diseasecode" in result.columns
        assert "product_value" in result.columns
        assert "disease_simple" in result.columns
        assert "disease_filter" in result.columns
        assert "diseasesort" in result.columns
        assert "secondary_disease_filter" in result.columns
        assert "diseasechoice_text" in result.columns
        assert "diseaseid" in result.columns
        assert "maindisease_value" in result.columns
        assert "globalhealtharea" in result.columns

    def test_updates_option_set_label(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        _, cleaned_option_sets = transform_diseases(df, option_sets=option_sets)
        assert "_optionset_new_globalhealtharea" in cleaned_option_sets
        os_df = cleaned_option_sets["_optionset_new_globalhealtharea"]
        labels = list(os_df["label"])
        assert "Womens Health" in labels
        assert "Sexual & reproductive health" not in labels

    def test_returns_cleaned_option_set_in_second_element(self):
        df = self._make_input_df()
        option_sets = self._make_option_sets()
        result, cleaned = transform_diseases(df, option_sets=option_sets)
        assert isinstance(result, pd.DataFrame)
        assert isinstance(cleaned, dict)
        assert len(cleaned) == 1

    def test_works_when_option_sets_is_none(self):
        df = self._make_input_df()
        result, cleaned = transform_diseases(df, option_sets=None)
        assert isinstance(result, pd.DataFrame)
        assert len(cleaned) == 0

    def test_does_not_modify_original(self):
        df = self._make_input_df()
        original_columns = list(df.columns)
        transform_diseases(df)
        assert list(df.columns) == original_columns

    def test_preserves_row_count(self):
        df = self._make_input_df()
        result, _ = transform_diseases(df)
        assert len(result) == 2

    def test_renames_diseasefilter_and_strips_whitespace(self):
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": ["  Malaria  ", "Kinetoplastid diseases "],
            }
        )
        result, _ = transform_diseases(df)
        assert "disease_filter" in result.columns
        # Legacy intermediate names are gone.
        assert "new_diseasefilter" not in result.columns
        assert "diseasefilter" not in result.columns
        assert result["disease_filter"].iloc[0] == "Malaria"
        assert result["disease_filter"].iloc[1] == "Kinetoplastid diseases"

    def test_collapses_secondary_sentinel_to_null(self):
        df = self._make_input_df(
            overrides={
                "new_secondary_diseae_choice_text": [
                    "P. falciparum",
                    "No secondary disease",
                ],
            }
        )
        result, _ = transform_diseases(df)
        assert "secondary_disease_name" in result.columns
        # Legacy intermediate name is gone.
        assert "secondary_diseae_choice_text" not in result.columns
        assert result["secondary_disease_name"].iloc[0] == "P. falciparum"
        assert pd.isna(result["secondary_disease_name"].iloc[1])

    def test_collapses_secondary_empty_string_to_null(self):
        df = self._make_input_df(
            overrides={
                "new_secondary_diseae_choice_text": ["  ", ""],
            }
        )
        result, _ = transform_diseases(df)
        assert pd.isna(result["secondary_disease_name"].iloc[0])
        assert pd.isna(result["secondary_disease_name"].iloc[1])

    def test_backfills_globalhealtharea_from_nd_flag(self):
        # Bronze gap recovery: when `new_globalhealtharea` arrives as
        # None (the SQLite-driver render of a TEXT NULL) but the row
        # carries the ND inclusion flag, we back-fill the option-set
        # code so the silver→gold OPTIONSET resolver produces the
        # right label.
        df = self._make_input_df(
            overrides={
                "new_globalhealtharea": [None, "100000002"],
                "new_incl_nd": [1, 0],
                "new_incl_eid": [0, 0],
            }
        )
        result, _ = transform_diseases(df)
        # Row 0: code was missing, ND flag set -> back-filled to 100000000.
        # Row 1: code already present -> unchanged.
        assert result["globalhealtharea"].iloc[0] == "100000000"
        assert result["globalhealtharea"].iloc[1] == "100000002"

    def test_backfills_globalhealtharea_from_eid_flag(self):
        # Zika is the canonical example: code missing in bronze but
        # `new_incl_eid = 1` marks it as an Emerging Infectious Disease.
        df = self._make_input_df(
            overrides={
                "new_globalhealtharea": [None, "100000000"],
                "new_incl_nd": [0, 1],
                "new_incl_eid": [1, 0],
            }
        )
        result, _ = transform_diseases(df)
        assert result["globalhealtharea"].iloc[0] == "100000001"
        assert result["globalhealtharea"].iloc[1] == "100000000"

    def test_globalhealtharea_stays_null_when_no_flag_is_set(self):
        # Rows like "R&D for all global health areas" have a missing
        # code AND no inclusion flag. They should stay NULL -- they're
        # cross-cutting buckets that don't belong to a single GHA.
        # Row 1 carries a populated code purely so the column is not
        # all-null in this fixture (otherwise `drop_empty_columns`
        # removes it -- a situation that never arises in production
        # because the column always has 448 populated rows).
        df = self._make_input_df(
            overrides={
                "new_globalhealtharea": [None, "100000002"],
                "new_incl_nd": [0, 0],
                "new_incl_eid": [0, 0],
            }
        )
        result, _ = transform_diseases(df)
        assert pd.isna(result["globalhealtharea"].iloc[0])
        assert result["globalhealtharea"].iloc[1] == "100000002"

    def test_existing_globalhealtharea_code_is_not_overwritten_by_flags(self):
        # If the source already provides an option-set code, the
        # back-fill must be a no-op even when the flags happen to
        # disagree. We never let derived flags rewrite an authoritative
        # source value.
        df = self._make_input_df(
            overrides={
                "new_globalhealtharea": ["100000001", "100000000"],
                "new_incl_nd": [1, 0],  # deliberately contradicts row 0
                "new_incl_eid": [0, 1],  # deliberately contradicts row 1
            }
        )
        result, _ = transform_diseases(df)
        assert result["globalhealtharea"].iloc[0] == "100000001"
        assert result["globalhealtharea"].iloc[1] == "100000000"

    def test_inclusion_flag_columns_are_not_in_silver_output(self):
        # The flags are consulted in-flight by the back-fill and then
        # dropped. They should not leak into silver because the code
        # column is the single source of truth downstream.
        df = self._make_input_df()
        result, _ = transform_diseases(df)
        assert "new_incl_nd" not in result.columns
        assert "new_incl_eid" not in result.columns
        # Also check the renamed variants don't appear by mistake.
        assert "incl_nd" not in result.columns
        assert "incl_eid" not in result.columns

    def test_backfills_filoviral_disease_filter(self):
        # Special Case: the parent-level Filoviral record arrives from the
        # CRM with a NULL disease_filter, making its linked priority
        # unreachable in the filter dropdown. The transform backfills it.
        df = self._make_input_df(
            overrides={
                "vin_name": [
                    "Filoviral diseases (including Ebola, Marburg) - Multiple filoviral diseases - Vaccines",
                    "Some other disease",
                ],
                "new_diseasefilter": [None, "Malaria"],
            }
        )
        result, _ = transform_diseases(df)
        assert result["disease_filter"].iloc[0] == "Filoviral diseases"
        # Other rows untouched.
        assert result["disease_filter"].iloc[1] == "Malaria"

    def test_filoviral_backfill_does_not_overwrite_existing_filter(self):
        # When a Filoviral row already has a disease_filter, leave it alone.
        df = self._make_input_df(
            overrides={
                "vin_name": [
                    "Filoviral diseases (including Ebola, Marburg) - Ebola - Vaccines",
                    "Other",
                ],
                "new_diseasefilter": ["Filoviral diseases", None],
            }
        )
        result, _ = transform_diseases(df)
        assert result["disease_filter"].iloc[0] == "Filoviral diseases"

    def test_normalizes_sti_primary_when_suffix_matches_secondary(self):
        # Three Bronze rows store new_diseasefilter as a parent-child
        # concatenation. Collapse only when the suffix exactly matches
        # the secondary text -- preserves any future legitimate value
        # that happens to contain " - ".
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": [
                    "Sexually transmitted infections (STIs) - Gonorrhea",
                    "Foo - Bar",  # not normalized: suffix doesn't match secondary
                ],
                "new_secondary_diseae_choice_text": [
                    "Gonorrhea",
                    "Different",
                ],
            }
        )
        result, _ = transform_diseases(df)
        assert (
            result["disease_filter"].iloc[0] == "Sexually transmitted infections (STIs)"
        )
        # Suffix didn't match -> primary unchanged.
        assert result["disease_filter"].iloc[1] == "Foo - Bar"
        # Secondary unchanged.
        assert result["secondary_disease_name"].iloc[0] == "Gonorrhea"
        assert result["secondary_disease_name"].iloc[1] == "Different"

    def test_disease_label_prefers_secondary(self):
        # Default rule: when a secondary disease exists, it is the label
        # (the primary group is implied by context).
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": ["Coronaviral diseases", "Filoviral diseases"],
                "new_secondary_diseae_choice_text": ["COVID-19", "Ebola"],
            }
        )
        result, _ = transform_diseases(df)
        assert result["disease_label"].iloc[0] == "COVID-19"
        assert result["disease_label"].iloc[1] == "Ebola"

    def test_disease_label_falls_back_to_primary_without_secondary(self):
        # No secondary -> show the primary disease group. The sentinel
        # "No secondary disease" has already been collapsed to NULL.
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": ["Tuberculosis", "Buruli ulcer"],
                "new_secondary_diseae_choice_text": [None, "No secondary disease"],
            }
        )
        result, _ = transform_diseases(df)
        assert result["disease_label"].iloc[0] == "Tuberculosis"
        assert result["disease_label"].iloc[1] == "Buruli ulcer"

    def test_disease_label_combines_for_malaria(self):
        # Malaria is the only outlier: its strains never stand alone, so
        # the label is "<primary> – <secondary>" with a spaced en dash.
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": ["Malaria", "Malaria"],
                "new_secondary_diseae_choice_text": ["P. falciparum", "P. vivax"],
            }
        )
        result, _ = transform_diseases(df)
        assert result["disease_label"].iloc[0] == "Malaria – P. falciparum"
        assert result["disease_label"].iloc[1] == "Malaria – P. vivax"

    def test_disease_label_for_sti_uses_secondary(self):
        # STIs arrive parent-collapsed with the specific infection in the
        # secondary field, so the default branch already prints it.
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": [
                    "Sexually transmitted infections (STIs) - Gonorrhea",
                    "Sexually transmitted infections (STIs)",
                ],
                "new_secondary_diseae_choice_text": [
                    "Gonorrhea",
                    "Trichomoniasis",
                ],
            }
        )
        result, _ = transform_diseases(df)
        assert result["disease_label"].iloc[0] == "Gonorrhea"
        assert result["disease_label"].iloc[1] == "Trichomoniasis"

    def test_disease_label_is_null_when_both_inputs_missing(self):
        # Cross-cutting "R&D for all global health areas" rows carry
        # neither a primary filter nor a secondary -> no label. The
        # second row keeps a secondary populated so the column survives
        # `drop_empty_columns` (in production it always has values).
        df = self._make_input_df(
            overrides={
                "new_diseasefilter": [None, "Dengue"],
                "new_secondary_diseae_choice_text": [None, "Severe dengue"],
            }
        )
        result, _ = transform_diseases(df)
        assert pd.isna(result["disease_label"].iloc[0])
        assert result["disease_label"].iloc[1] == "Severe dengue"

    def test_backfills_secondary_for_multiple_filoviral_diseases(self):
        # "Multiple filoviral diseases" rows have NULL secondary, making them
        # collapse into the parent in the hierarchy query. The transform
        # backfills secondary_disease_name so they appear as a distinct child.
        df = self._make_input_df(
            overrides={
                "vin_name": [
                    "Filoviral diseases (including Ebola, Marburg)"
                    " - Multiple filoviral diseases - Vaccines",
                    "Filoviral diseases (including Ebola, Marburg)"
                    " - Ebola - Vaccines",
                ],
                "new_diseasefilter": [None, "Filoviral diseases"],
                # Row 1 keeps a non-null secondary so the column survives
                # drop_empty_columns (in production other rows populate it).
                "new_secondary_diseae_choice_text": [None, "Ebola"],
            }
        )
        result, _ = transform_diseases(df)
        # Row 0: name contains "Multiple filoviral diseases" -> backfilled.
        assert (
            result["secondary_disease_name"].iloc[0]
            == "Multiple filoviral diseases"
        )
        # Row 1: already has a secondary ("Ebola") -> not overwritten.
        assert result["secondary_disease_name"].iloc[1] == "Ebola"
        # disease_label should reflect the backfilled secondary.
        assert (
            result["disease_label"].iloc[0] == "Multiple filoviral diseases"
        )
