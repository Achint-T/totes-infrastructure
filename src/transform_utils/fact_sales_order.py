import pandas as pd

# Inputs: Pandas dataframe for sales_order 
# Create new dataframe with data that has been transformed
# Return new dataframe 



def util_fact_sales_order(df_sales_order):
    df_fact_sales_order = pd.DataFrame()

    # df_fact_sales_orders['sales_record_id'] = df_sales_order['sales_record_id']
    df_fact_sales_order['sales_order_id'] = df_sales_order['sales_order_id']

    df_fact_sales_order['created_date'] = pd.to_datetime(df_sales_order['created_at']).dt.date
    df_fact_sales_order['created_time'] = pd.to_datetime(df_sales_order['created_at']).dt.time
    df_fact_sales_order['last_updated_date'] = pd.to_datetime(df_sales_order['last_updated']).dt.date
    df_fact_sales_order['last_updated_time'] = pd.to_datetime(df_sales_order['last_updated']).dt.time

    df_fact_sales_order['sales_staff_id'] = df_sales_order['staff_id']
    df_fact_sales_order['counterparty_id'] = df_sales_order['counterparty_id']
    df_fact_sales_order['units_sold'] = df_sales_order['units_sold']
    df_fact_sales_order['unit_price'] = pd.to_numeric(df_sales_order['unit_price']).round(2)
    df_fact_sales_order['currency_id'] = df_sales_order['currency_id']
    df_fact_sales_order['design_id'] = df_sales_order['design_id']

    df_fact_sales_order['agreed_payment_date'] = pd.to_datetime(df_sales_order['agreed_payment_date']).dt.date
    df_fact_sales_order['agreed_delivery_date'] = pd.to_datetime(df_sales_order['agreed_delivery_date']).dt.date
    df_fact_sales_order['agreed_delivery_location_id'] = df_sales_order['agreed_delivery_location_id']

    df_fact_sales_order.index.name = 'sales_record_id'
    
    return df_fact_sales_order

