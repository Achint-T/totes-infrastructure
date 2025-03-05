from src.transform_utils.dim_currency import *
import pandas as pd
import pytest

class TestDimCurrency:

    def test_returns_dataframe(self):

        data = [
            [1, 'GBP', '', ''], 
            [2, 'USD', '', ''], 
            [3, 'EUR', '', '']
        ]

        output = pd.DataFrame(data, columns=["currency_id", "currency_code", "created_at", "last_updated"])

        response = util_dim_currency(output)

        assert isinstance(response, pd.DataFrame)
    
    def test_returns_correct_columns(self):

        data = [
            [1, 'GBP', '', ''], 
            [2, 'USD', '', ''], 
            [3, 'EUR', '', '']
        ]

        output = pd.DataFrame(data, columns=["currency_id", "currency_code", "created_at", "last_updated"])

        response = util_dim_currency(output)

        assert response.index.name == 'currency_id'
        assert list(response.columns) == ['currency_id', 'currency_code', 'currency_name']


    def test_returns_correct_currency_name_values(self):

        data = [
            [1, 'GBP', '', ''], 
            [2, 'USD', '', ''], 
            [3, 'EUR', '', '']
        ]

        output = pd.DataFrame(data, columns=["currency_id", "currency_code", "created_at", "lasted_updated"])

        response = util_dim_currency(output)

        lst_correct_currency_name_values = ["British Pound", "US Dollar", "Euro"]

        assert response['currency_name'].tolist() == lst_correct_currency_name_values

class TestDimCurrencyErrorHandling:

    def test_empty_dataframe_currency(self):

        test_df_currency = pd.DataFrame()

        response = util_dim_currency(test_df_currency)

        assert response == "The source dataframe currency is empty"

    def test_missing_colunn_currency_code(self):

        data = [
            [1, '', ''], 
            [2, '', ''], 
            [3, '', '']
        ]

        test_df_currency = pd.DataFrame(data, columns=['currency_id', 'created_at', 'last_updated'])

        response = util_dim_currency(test_df_currency)

        assert response == "Error: Missing columns currency_code from the source dataframe"
