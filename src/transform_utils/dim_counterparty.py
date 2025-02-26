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

    # Merge dataframes and remove unneeded columns to create df_dim_counterparty
    df_dim_counterparty = df_fact_sales_order[['counterparty_id']].merge(
        df_counterparty[['counterparty_id', 'counterparty_legal_name', 'legal_address_id']], 
        on='counterparty_id', 
        how='left'
    ).merge(
        df_address[['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']],
        left_on='legal_address_id', 
        right_on='address_id',
        how='left'
    )

    df_dim_counterparty.drop(columns=['address_id'], inplace=True)
    df_dim_counterparty.drop(columns=['legal_address_id'], inplace=True)

    # Rename columns
    df_dim_counterparty.rename(columns={
        'counterparty_legal_name': 'counterparty_legal_name',
        'address_line_1': 'counterparty_legal_address_line_1',
        'address_line_2': 'counterparty_legal_address_line_2',
        'district': 'counterparty_legal_district',
        'city': 'counterparty_legal_city',
        'postal_code': 'counterparty_legal_postal_code',
        'country': 'counterparty_legal_country',
        'phone': 'counterparty_legal_phone_number'
    }, inplace=True)

    df_dim_counterparty.index.name = None

    return df_dim_counterparty
