from src.transform_utils.fact_sales_order import *
import pandas as pd
import pytest 


def test_returns_dataframe():
    data = [
        [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
        [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
        [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
        ]
    
    test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

    output = util_fact_sales_order(test_df)

    assert isinstance(output, pd.DataFrame)

def test_returns_correct_columns():
    data = [
        [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
        [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
        [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
        ]
    
    test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

    output = util_fact_sales_order(test_df)
    
    assert output.index.name == 'sales_record_id'
    assert list(output.columns) == ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']

# Test commented out as datatypes will be converted at load stage 

# def test_column_datatypes():
#     data = [
#         [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
#         [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
#         [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
#         ]
    
#     test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

#     output = util_fact_sales_order(test_df)

#     assert output.dtypes == test_df.dtypes