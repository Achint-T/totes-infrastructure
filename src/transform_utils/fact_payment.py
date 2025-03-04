import pandas as pd

def util_fact_payment(df_payment):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse.
       
       Args:
       df_payment: The dataframe created from the payment table csv.
       
       Returns:
       df_fact_payment - a dataframe ready to be converted to parquet file."""
    
    df_fact_payment = pd.DataFrame()

    required_columns = [
        "payment_id",
        "created_at",
        "last_updated",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date"
    ]

    if df_payment.empty:
        return "Error: The source dataframe for fact_payment is empty"
    col_missing = [col for col in required_columns if col not in df_payment.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)}"
    
    df_fact_payment.index.name = "payment_record_id"
    df_fact_payment["payment_id"] = df_payment["payment_id"]

    df_fact_payment["created_date"] = pd.to_datetime(df_payment["created_at"], errors='coerce').dt.date
    df_fact_payment["created_time"] = pd.to_datetime(df_payment["created_at"], errors='coerce').dt.time
    df_fact_payment["last_updated_date"] = pd.to_datetime(df_payment["last_updated"], errors='coerce').dt.date
    df_fact_payment["last_updated_time"] = pd.to_datetime(df_payment["last_updated"], errors='coerce').dt.time

    df_fact_payment["transaction_id"] = df_payment["transaction_id"]
    df_fact_payment["counterparty_id"] = df_payment["counterparty_id"]
    df_fact_payment["payment_amount"] = df_payment["payment_amount"]
    df_fact_payment["currency_id"] = df_payment["currency_id"]
    df_fact_payment["payment_type_id"] = df_payment["payment_type_id"]
    df_fact_payment["paid"] = df_payment["paid"]
    df_fact_payment["payment_date"] = df_payment["payment_date"]

    return df_fact_payment