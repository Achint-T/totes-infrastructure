from src.transform_utils.fact_sales_order import *
from src.transform_utils.dim_location import *
import pandas as pd
import pytest 

class TestDimLocation:

    def test_returns_dataframe(self):

        data = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'LONDON', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'LONDON', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'LONDON', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']
            ]

        output = pd.DataFrame(data, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        response = util_dim_location(output)

        assert isinstance(response, pd.DataFrame)

    def test_returns_correct_columns(self): 

        data = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'LONDON', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'LONDON', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'LONDON', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']]

        output = pd.DataFrame(data, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        response = util_dim_location(output)

        assert response.index.name == 'location_id'
        assert list(response.columns) == ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']

class TestDimLocationErrorHandling:
    
    def test_empty_dataframe_address(self):

        test_df_address = pd.DataFrame()

        response = util_dim_location(test_df_address)

        assert response == "The source dataframe address is empty"
    
    def test_missing_column_city(self):

        data = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']]

        test_df_address = pd.DataFrame(data, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        response = util_dim_location(test_df_address)

        assert response == "Error: Missing columns city for the source data frame address"

