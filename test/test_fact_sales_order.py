from src.transform_utils.fact_sales_order import *
import pandas as pd
import pytest 

class TestSalesOrder:

    def test_returns_dataframe(self):
        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
            ]
        
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        output = util_fact_sales_order(test_df)

        assert isinstance(output, pd.DataFrame)


    def test_returns_correct_columns(self):
        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
            ]
        
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        output = util_fact_sales_order(test_df)

        assert list(output.columns) == ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']


    def test_datetime_price_columns_transformed(valid_df):
        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
            ]
        
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        output = util_fact_sales_order(test_df)

        assert output['created_date'].iloc[0] == pd.to_datetime('2022-11-03 14:20:52.186').date()
        assert output['created_time'].iloc[0] == pd.to_datetime('2022-11-03 14:20:52.186').time()
        assert output['last_updated_date'].iloc[0] == pd.to_datetime('2022-11-03 14:20:52.186').date()
        assert output['last_updated_time'].iloc[0] == pd.to_datetime('2022-11-03 14:20:52.186').time()
        assert output['unit_price'].iloc[0] == 3.94


class TestSalesOrderErrorHandling:

    def test_empty_dataframe(self):
        test_df = pd.DataFrame()
        output = util_fact_sales_order(test_df)
        assert output == "The source dataframe for fact_sales_order is empty"


    def test_missing_columns(self):

        data = [
            ['2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            ['2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            ['2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
            ]
        
        test_df = pd.DataFrame(data, columns=['created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        output = util_fact_sales_order(test_df)
        assert output == "Error: Missing columns sales_order_id"


    def test_invalid_datetime_value(self):
        data = [
            [1, 'date string', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
            ]
        
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        with pytest.raises(Exception):
            util_fact_sales_order(test_df)


    def test_invalid_numeric_value(self):
        data = [
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 'price', 3, '2022-11-07', '2022-11-08', 2]
            ]
        
        test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

        with pytest.raises(Exception):
            util_fact_sales_order(test_df)