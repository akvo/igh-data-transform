"""Tests for priorities transformation."""

import pandas as pd

from igh_data_transform.transformations.priorities import transform_priorities


class TestTransformPriorities:
    """Tests for transform_priorities function."""

    def _make_input_df(self, overrides=None):
        """Create a minimal input DataFrame mimicking vin_rdpriorities."""
        data = {
            "row_id": [1, 2],
            "versionnumber": [100, 200],
            "crc8b_includeinipps": [None, None],
            "vin_name": ["10001", "10002"],
            "_owninguser_value": ["user-1", "user-2"],
            "new_safety": ["Safe", "Also safe"],
            "new_targetpopulation": ["All ages", "Adults"],
            "_vin_disease_value": ["disease-1", "disease-2"],
            "vin_rdpriorityid": ["id-1", "id-2"],
            "statecode": [0, 0],
            "new_publicationdate": ["2023-01-01", "2024-01-01"],
            "_ownerid_value": ["owner-1", "owner-2"],
            "new_efficacy": ["High", "Medium"],
            "new_indication": ["Malaria", "TB"],
            "createdon": ["2023-01-01", "2024-01-01"],
            "modifiedon": ["2023-06-01", "2024-06-01"],
            "importsequencenumber": [None, None],
            "_modifiedby_value": ["mod-1", "mod-2"],
            "new_intendeduse": ["Prevention", "Treatment"],
            "crc8b_addedclinicalvalue": [None, None],
            "crc8b_p2iproductlaunchbasedonrdpriority": [None, None],
            "new_source": ["WHO", "MMV"],
            "_createdby_value": ["created-1", "created-2"],
            "_vin_secondarydisease_value": [None, "disease-3"],
            "statuscode": [1, 1],
            "timezoneruleversionnumber": [0.0, 0.0],
            "new_ppctitle": ["TPP: Test 1", "TPP: Test 2"],
            "crc8b_realisticlaunch": [None, None],
            "_vin_product_value": ["prod-1", "prod-2"],
            "_owningbusinessunit_value": ["bu-1", "bu-1"],
            "new_author": ["WHO", "World Health Organization"],
            "json_response": ['{"key": "val"}', '{"key": "val2"}'],
            "sync_time": ["2026-01-09T12:00:00", "2026-01-09T12:00:01"],
            "valid_from": ["2023-01-01", "2024-01-01"],
            "valid_to": [None, None],
            # Columns that will be all-null (dropped by drop_empty_columns)
            "_createdonbehalfby_value": [None, None],
            "overriddencreatedon": [None, None],
            "utcconversiontimezonecode": [None, None],
            "crc8b_addedclinicalvaluedescription": [None, None],
            "_owningteam_value": [None, None],
            "_modifiedonbehalfby_value": [None, None],
        }
        if overrides:
            data.update(overrides)
        return pd.DataFrame(data)

    def test_drops_metadata_columns(self):
        df = self._make_input_df()
        result, _ = transform_priorities(df)
        dropped = [
            "row_id", "crc8b_includeinipps", "_owninguser_value",
            "statecode", "_ownerid_value", "importsequencenumber",
            "_modifiedby_value", "crc8b_addedclinicalvalue",
            "crc8b_p2iproductlaunchbasedonrdpriority",
            "_createdby_value", "statuscode", "timezoneruleversionnumber",
            "crc8b_realisticlaunch", "_owningbusinessunit_value",
            "json_response", "sync_time",
        ]
        for col in dropped:
            assert col not in result.columns

    def test_drops_empty_columns_preserving_valid_to(self):
        df = self._make_input_df()
        result, _ = transform_priorities(df)
        assert "valid_to" in result.columns
        assert "_createdonbehalfby_value" not in result.columns
        assert "_owningteam_value" not in result.columns

    def test_renames_columns(self):
        df = self._make_input_df()
        result, _ = transform_priorities(df)
        assert "name" in result.columns
        assert "vin_name" not in result.columns
        assert "safety" in result.columns
        assert "new_safety" not in result.columns
        assert "targetpopulation" in result.columns
        assert "diseasevalue" in result.columns
        assert "rdpriorityid" in result.columns
        assert "publicationdate" in result.columns
        assert "efficacy" in result.columns
        assert "indication" in result.columns
        assert "intendeduse" in result.columns
        assert "source" in result.columns
        assert "secondarydiseasevalue" in result.columns
        assert "ppctitle" in result.columns
        assert "product_value" in result.columns
        assert "author" in result.columns

    def test_standardizes_author(self):
        df = self._make_input_df()
        result, _ = transform_priorities(df)
        assert list(result["author"]) == ["WHO", "WHO"]

    def test_returns_tuple_with_empty_dict(self):
        df = self._make_input_df()
        result, option_sets = transform_priorities(df)
        assert isinstance(result, pd.DataFrame)
        assert isinstance(option_sets, dict)
        assert len(option_sets) == 0

    def test_does_not_modify_original(self):
        df = self._make_input_df()
        original_columns = list(df.columns)
        transform_priorities(df)
        assert list(df.columns) == original_columns

    def test_preserves_row_count(self):
        df = self._make_input_df()
        result, _ = transform_priorities(df)
        assert len(result) == 2

    def test_works_with_option_sets_param(self):
        df = self._make_input_df()
        result, option_sets = transform_priorities(df, option_sets={"some_set": pd.DataFrame()})
        assert isinstance(result, pd.DataFrame)
        assert len(option_sets) == 0
