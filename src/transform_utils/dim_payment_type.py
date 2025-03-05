import pandas as pd

def util_dim_payment_type(df_payment_type):
    df_dim_payment_type = pd.DataFrame()

    df_dim_payment_type["payment_type_id"] = df_payment_type["payment_type_id"]
    df_dim_payment_type["payment_type_name"] = df_payment_type["payment_type_name"]

    return df_dim_payment_type