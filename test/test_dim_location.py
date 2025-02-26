from src.transform_utils.fact_sales_order import *
from src.transform_utils.dim_location import *
import pandas as pd
import pytest 

class TestDimLocation:

    def test_returns_dataframe(self):
        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 27], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 33], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 45]
            ]
    
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        output = util_fact_sales_order(test_df) 

        data_ii = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'LONDON', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'LONDON', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'LONDON', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']
            ]

        output_ii = pd.DataFrame(data_ii, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        response = util_dim_location(output, output_ii)

        assert isinstance(response, pd.DataFrame)

    def test_returns_correct_columns(self):
        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 27], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 33], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 45]
            ]
    
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        output = util_fact_sales_order(test_df) 

        data_ii = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'LONDON', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'LONDON', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'LONDON', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']]

        output_ii = pd.DataFrame(data_ii, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        response = util_dim_location(output, output_ii)

        assert response.index.name == 'location_id'
        assert list(response.columns) == ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']

class TestDimLocationErrorHandling:

    def test_empty_dataframe_fact_sales_order(self):

        data = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'LONDON', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'LONDON', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'LONDON', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']
            ]

        test_df_address = pd.DataFrame(data, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        test_df_transformed_fact_sales_order = pd.DataFrame()

        response = util_dim_location(test_df_transformed_fact_sales_order, test_df_address)

        assert response == "The source dataframe fact_sales_order is empty"
    
    def test_empty_dataframe_address(self):

        test_df_address = pd.DataFrame()

        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 27], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 33], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 45]
            ]
    
        test_df_sales_order = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])
        test_df_transformed_fact_sales_order = util_fact_sales_order(test_df_sales_order) 

        response = util_dim_location(test_df_transformed_fact_sales_order, test_df_address)

        assert response == "The source dataframe address is empty"
    
    def test_missing_column_city(self):

        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 27], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 33], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 45]
            ]
    
        test_df_sales_order = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])
        test_df_transformed_fact_sales_order = util_fact_sales_order(test_df_sales_order) 

        data_ii = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']]

        test_df_address = pd.DataFrame(data_ii, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])

        response = util_dim_location(test_df_transformed_fact_sales_order, test_df_address)

        assert response == "Error: Missing columns city for the source data frame address"

