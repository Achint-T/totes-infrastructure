from src.transform_utils.dim_design import *
import pandas as pd

class TestDimDesign:

    def test_returns_data_frame(self):
        data = [
            [1, '2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186'], 
            [2, '2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186'], 
            [3, '2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186']
            ]
        
        test_df = pd.DataFrame(data, columns=["design_id","created_at", "last_updated","design_name","file_location","file_name"])
        output = util_dim_design(test_df)

        assert isinstance(output, pd.DataFrame)

    def test_returns_correct_columns(self):
        data = [
            [1, '2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186'], 
            [2, '2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186'], 
            [3, '2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186']
            ]
        
        test_df = pd.DataFrame(data, columns=["design_id","created_at", "last_updated","design_name","file_location","file_name"])
        output = util_dim_design(test_df)

        assert list(output.columns) == ["design_id",
        "design_name",
        "file_location",
        "file_name",]

class TestSalesOrderErrorHandling:

    def test_empty_dataframe(self):
        test_df = pd.DataFrame()
        output = util_dim_design(test_df)
        assert output == "The source dataframe for dim_design is empty"

    def test_missing_columns(self):
        data = [
            ['2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186'], 
            ['2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186'], 
            ['2022-11-03 14:20:52.186', 'test-name', 'test-file-location', 'test-file-name', '2022-11-03 14:20:52.186']
            ]
        
        test_df = pd.DataFrame(data, columns=["created_at", "last_updated","design_name","file_location","file_name"])
        output = util_dim_design(test_df)
        assert output == "Error: Missing columns design_id"