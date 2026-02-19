"""Tests for dim_disease transformation: disease_group_name from source 'disease' column."""

from unittest.mock import MagicMock

from src.transformer import Transformer


def _make_transformer(disease_rows):
    """Build a Transformer whose extractor yields the given disease rows."""
    mock_extractor = MagicMock()

    def extract_table(table, columns=None):
        if table == "vin_diseases":
            return iter(disease_rows)
        return iter([])

    mock_extractor.extract_table = MagicMock(side_effect=extract_table)
    return Transformer(mock_extractor)


class TestDimDiseaseGroupName:
    """Test disease_group_name column in dim_disease."""

    def test_disease_group_name_populated_from_source(self):
        """disease_group_name is sourced from the 'disease' column."""
        rows = [
            {
                "diseaseid": "d1",
                "name": "Dengue - Vaccines",
                "disease": "Dengue",
                "globalhealtharea": 1,
                "disease_simple": "NTD",
            },
        ]
        t = _make_transformer(rows)
        t.extractor.lookup_optionset = MagicMock(return_value="NTDs")

        result = t.transform_dimension("dim_disease")
        assert len(result) == 1
        assert result[0]["disease_group_name"] == "Dengue"

    def test_disease_group_name_differs_from_disease_name(self):
        """disease_name has product-type suffix; disease_group_name does not."""
        rows = [
            {
                "diseaseid": "d1",
                "name": "Dengue - Diagnostics",
                "disease": "Dengue",
                "globalhealtharea": 1,
                "disease_simple": "NTD",
            },
            {
                "diseaseid": "d2",
                "name": "Dengue - Drugs",
                "disease": "Dengue",
                "globalhealtharea": 1,
                "disease_simple": "NTD",
            },
        ]
        t = _make_transformer(rows)
        t.extractor.lookup_optionset = MagicMock(return_value="NTDs")

        result = t.transform_dimension("dim_disease")
        for row in result:
            assert row["disease_group_name"] == "Dengue"
            assert row["disease_name"] != row["disease_group_name"]

    def test_disease_group_name_none_when_missing(self):
        """disease_group_name is None when source 'disease' column is absent."""
        rows = [
            {
                "diseaseid": "d1",
                "name": "Unknown Disease",
                "globalhealtharea": 1,
                "disease_simple": "Unknown",
            },
        ]
        t = _make_transformer(rows)
        t.extractor.lookup_optionset = MagicMock(return_value="Other")

        result = t.transform_dimension("dim_disease")
        assert len(result) == 1
        assert result[0]["disease_group_name"] is None
