import pandas as pd

def util_dim_transaction(df_transaction):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse.
       
       Args:
       df_transaction: The dataframe created from the transaction table csv.
       
       Returns:
       df_dim_transaction - a dataframe ready to be converted to parquet file."""
    
    df_dim_transaction = pd.DataFrame()

    if df_transaction.empty:
        return "Error: The source transaction dataframe is empty"
    
    required_columns = [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id"
    ]

    col_missing = [col for col in required_columns if col not in df_transaction.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)} for the source transaction dataframe"
    
    df_dim_transaction["transaction_id"] = df_transaction["transaction_id"]
    df_dim_transaction["transaction_type"] = df_transaction["transaction_type"]
    df_dim_transaction["sales_order_id"] = df_transaction["sales_order_id"]
    df_dim_transaction["purchase_order_id"] = df_transaction["purchase_order_id"]

    return df_dim_transaction