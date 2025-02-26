import pandas as pd 
from src.transform_utils.fact_sales_order import util_fact_sales_order

data = [
        [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 27], 
        [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 33], 
        [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 45]
        ]
    
test_df = pd.DataFrame(data, columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'])

# print(test_df)

output = util_fact_sales_order(test_df)

# print(output)

#####

data_ii = [
            [1, '172 BROOKSCROFT ROAD', 'WALTHAMSTOW', 'WALTHAM FOREST', 'LONDON', 'E17 4JR', 'UNITED KINGDOM', '02085230568', '', ''],
            [2, '1 DUKES ROAD', 'EAST HAM', 'NEWHAM', 'LONDON', 'E6 2NU', 'UNITED KINGDOM', '07510561203', '', ''],
            [3, '104 SHACKLEWELL LANE', 'DALSTON', 'HACKNEY', 'LONDON', 'E8 2JS', 'UNITED KINGDOM', '07725138808', '', '']
]

output_ii = pd.DataFrame(data_ii, columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])
# print(output_ii)


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

df_dim_location = util_dim_location(output, output_ii)

print(df_dim_location)