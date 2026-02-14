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
            "_optionset_new_globalhealtharea": pd.DataFrame({
                "code": [100000000, 100000001, 100000002],
                "label": [
                    "Neglected disease",
                    "Emerging infectious disease",
                    "Sexual & reproductive health",
                ],
                "first_seen": ["2026-01-09", "2026-01-09", "2026-01-09"],
            }),
        }

    def test_drops_metadata_columns(self):
        df = self._make_input_df()
        result, _ = transform_diseases(df)
        dropped = [
            "row_id", "createdon", "modifiedon", "_organizationid_value",
            "crc8b_addedclinicalvalue", "crc8b_tppppc",
            "crc8b_addedclinicalvaluedescription", "crc8b_p2iproductlaunch",
            "statuscode", "statecode", "_createdby_value",
            "new_globalhealthareaportal", "importsequencenumber",
            "new_incl_eid", "_modifiedby_value", "new_incl_nd",
            "json_response", "sync_time",
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
        assert "secondary_diseae_choice_text" in result.columns
        assert "diseasecode" in result.columns
        assert "product_value" in result.columns
        assert "disease_simple" in result.columns
        assert "diseasefilter" in result.columns
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
