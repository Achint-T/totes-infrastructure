import pandas as pd 
import requests

def util_dim_currency(df_currency):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse. 

       Args:
       df_abc: The data frame created from the currency table csv 

       Returns:
       df_dim_currency - a dataframe ready to be converted to a parquet file
    """

    df_dim_currency = pd.DataFrame()

    if df_currency.empty:
        return "The source dataframe currency is empty"

    required_columns = ['currency_id', 'currency_code']
    col_missing = [col for col in required_columns if col not in df_currency.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)} from the source dataframe"

    df_dim_currency['currency_id'] = df_currency['currency_id']
    df_dim_currency['currency_code'] = df_currency['currency_code']

    URL = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"
    curr_rates = requests.get(url=URL)
    dict_curr_rates = curr_rates.json()

    lst_currency_code_values = df_dim_currency['currency_code'].tolist()
    lst_currency_name_values = [dict_curr_rates[currency_code_value.lower()] for currency_code_value in lst_currency_code_values]

    df_dim_currency['currency_name'] = lst_currency_name_values
    df_dim_currency.index.name = "currency_id"
    return df_dim_currency


