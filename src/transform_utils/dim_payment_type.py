import pandas as pd

def util_dim_payment_type(df_payment_type):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse.
       
       Args:
       df_payment_type: The dataframe created from the transaction table csv.
       
       Returns:
       df_dim_payment_type - a dataframe ready to be converted to parquet file."""
    
    df_dim_payment_type = pd.DataFrame()

    required_columns = [
        "payment_type_id",
        "payment_type_name"
    ]

    if df_payment_type.empty:
        return "Error: The source payment type dataframe is empty"
    missing_cols = [col for col in required_columns if col not in df_payment_type.columns]
    if missing_cols:
        return f"Error: Missing columns {', '.join(missing_cols)}"

    df_dim_payment_type["payment_type_id"] = df_payment_type["payment_type_id"]
    df_dim_payment_type["payment_type_name"] = df_payment_type["payment_type_name"]

    return df_dim_payment_type