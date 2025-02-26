import pandas as pd 
from src.transform_utils.fact_sales_order import util_fact_sales_order

# Inputs: Pandas dataframe from the ingested address table
# Process: Create new dataframe with data that has been transformed 
# Output: Return newly transformed dim_location dataframe

def util_dim_location(df_fact_sales_order, df_address):
    df_dim_location = pd.DataFrame()

    df_dim_location['location_id'] = df_fact_sales_order['agreed_delivery_location_id']
    df_dim_location['address_line_1'] = df_address['address_line_1']
    df_dim_location['address_line_2'] = df_address['address_line_2']
    df_dim_location['district'] = df_address['district']
    df_dim_location['city'] = df_address['city']
    df_dim_location['postal_code'] = df_address['postal_code']
    df_dim_location['country'] = df_address['country']
    df_dim_location['phone'] = df_address['phone']

    df_dim_location.index.name = "location_id"

    return df_dim_location