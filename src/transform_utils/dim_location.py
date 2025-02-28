import pandas as pd 

# Inputs: Pandas dataframe from the ingested address table
# Process: Create new dataframe with data that has been transformed 
# Output: Return newly transformed dim_location dataframe

def util_dim_location(df_address):
    df_dim_location = pd.DataFrame()

    if df_address.empty:
        return "The source dataframe address is empty"
    
    required_columns = ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated']
    col_missing = [col for col in required_columns if col not in df_address.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)} for the source data frame address"

    df_dim_location['location_id'] = df_address['address_id']
    df_dim_location['address_line_1'] = df_address['address_line_1']
    df_dim_location['address_line_2'] = df_address['address_line_2']
    df_dim_location['district'] = df_address['district']
    df_dim_location['city'] = df_address['city']
    df_dim_location['postal_code'] = df_address['postal_code']
    df_dim_location['country'] = df_address['country']
    df_dim_location['phone'] = df_address['phone']

    df_dim_location.index.name = "location_id"

    return df_dim_location