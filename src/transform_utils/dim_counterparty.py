import pandas as pd


def util_dim_counterparty(df_fact_sales_order, df_counterparty, df_address):
    df_dim_counterparty = pd.DataFrame()

    counterparty_required_cols = ["counterparty_id"]
    address_required_cols = [
        "address_id",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
        "created_at",
        "last_updated",
    ]

    # Error handling: empty dataframe, missing columns
    if df_fact_sales_order.empty or df_counterparty.empty or df_address.empty:
        return "Error: One or more of the source dataframes is empty"
    col_missing_counterparty = [col for col in counterparty_required_cols if col not in df_counterparty.columns]
    col_missing_address = [col for col in address_required_cols if col not in df_address.columns]
    if col_missing_counterparty != [] or col_missing_address != []:
        return f"Error: Missing columns {', '.join(col_missing_counterparty)}{', '.join(col_missing_address)}"

    # Filling in the dataframe
    df_dim_counterparty["counterparty_id"] = df_fact_sales_order["counterparty_id"]
    df_dim_counterparty["counterparty_legal_name"] = df_counterparty[
        "counterparty_legal_name"
    ]
    df_dim_counterparty["counterparty_legal_address_line_1"] = df_address[
        "address_line_1"
    ]
    df_dim_counterparty["counterparty_legal_address_line_2"] = df_address[
        "address_line_2"
    ]
    df_dim_counterparty["counterparty_legal_district"] = df_address["district"]
    df_dim_counterparty["counterparty_legal_city"] = df_address["city"]
    df_dim_counterparty["counterparty_legal_postal_code"] = df_address["postal_code"]
    df_dim_counterparty["counterparty_legal_country"] = df_address["country"]
    df_dim_counterparty["counterparty_legal_phone_number"] = df_address["phone"]

    df_dim_counterparty.index.name = None

    return df_dim_counterparty
