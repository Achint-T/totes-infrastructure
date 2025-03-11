import pandas as pd

def util_fact_purchase_order(df_purchase_order):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse.
       
       Args:
       df_purchase_order: The dataframe created from the purchase order table csv.
       
       Returns:
       df_fact_purchase_order - a dataframe ready to be converted to parquet file."""

    df_fact_purchase_order = pd.DataFrame()

    required_columns = [
        "purchase_order_id",
        "created_at",
        "last_updated",
        "staff_id",
        "counterparty_id",
        "item_code",
        "item_quantity",
        "item_unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id"
    ]

    if df_purchase_order.empty:
        return "The source dataframe for fact_purchase_order is empty"
    col_missing = [col for col in required_columns if col not in df_purchase_order.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)}"

    df_fact_purchase_order["purchase_order_id"] = df_purchase_order["purchase_order_id"]

    df_fact_purchase_order["created_date"] = pd.to_datetime(df_purchase_order["created_at"], format='mixed').dt.date
    df_fact_purchase_order["created_time"] = pd.to_datetime(df_purchase_order["created_at"], format='mixed').dt.time
    
    df_fact_purchase_order["last_updated_date"] = pd.to_datetime(df_purchase_order["last_updated"], format='mixed').dt.date
    df_fact_purchase_order["last_updated_time"] = pd.to_datetime(df_purchase_order["last_updated"], format='mixed').dt.time

    for column in required_columns[3:7]:
        df_fact_purchase_order[column] = df_purchase_order[column]

    df_fact_purchase_order["item_unit_price"] = pd.to_numeric(df_purchase_order["item_unit_price"])

    for column in required_columns[8:]:
        df_fact_purchase_order[column] = df_purchase_order[column]

    return df_fact_purchase_order