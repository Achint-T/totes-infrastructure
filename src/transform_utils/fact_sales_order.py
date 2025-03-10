import pandas as pd

def util_fact_sales_order(df_sales_order):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse.
       
       Args:
       df_sales_order: The dataframe created from the sales order table csv.
       
       Returns:
       df_fact_sales_order - a dataframe ready to be converted to parquet file."""

    df_fact_sales_order = pd.DataFrame()

    required_columns = [
        "sales_order_id",
        "created_at",
        "last_updated",
        "staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    if df_sales_order.empty:
        return "The source dataframe for fact_sales_order is empty"
    col_missing = [col for col in required_columns if col not in df_sales_order.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)}"

    df_fact_sales_order["sales_order_id"] = df_sales_order["sales_order_id"]

    df_fact_sales_order["created_date"] = pd.to_datetime(df_sales_order["created_at"], errors='coerce').dt.date
    df_fact_sales_order["created_time"] = pd.to_datetime(
        df_sales_order["created_at"], errors='coerce'
    ).dt.time
    df_fact_sales_order["last_updated_date"] = pd.to_datetime(
        df_sales_order["last_updated"], errors='coerce'
    ).dt.date
    df_fact_sales_order["last_updated_time"] = pd.to_datetime(
        df_sales_order["last_updated"], errors='coerce'
    ).dt.time

    df_fact_sales_order["sales_staff_id"] = df_sales_order["staff_id"]
    df_fact_sales_order["counterparty_id"] = df_sales_order["counterparty_id"]
    df_fact_sales_order["units_sold"] = df_sales_order["units_sold"]
    df_fact_sales_order["unit_price"] = pd.to_numeric(
        df_sales_order["unit_price"], errors='coerce'
    ).round(2)
    df_fact_sales_order["currency_id"] = df_sales_order["currency_id"]
    df_fact_sales_order["design_id"] = df_sales_order["design_id"]

    df_fact_sales_order["agreed_payment_date"] = pd.to_datetime(
        df_sales_order["agreed_payment_date"], errors='coerce'
    ).dt.date
    df_fact_sales_order["agreed_delivery_date"] = pd.to_datetime(
        df_sales_order["agreed_delivery_date"], errors='coerce'
    ).dt.date
    df_fact_sales_order["agreed_delivery_location_id"] = df_sales_order["agreed_delivery_location_id"]

    # df_fact_sales_order.index.name = "sales_record_id"

    return df_fact_sales_order
