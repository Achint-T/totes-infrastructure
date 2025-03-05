from src.transform_utils.dim_transaction import util_dim_transaction
import pandas as pd
import pytest

@pytest.fixture
def transaction():
    data = {
        "transaction_id": [1, 2, 3],
        "transaction_type": ["PURCHASE", "PURCHASE", "SALE"],
        "sales_order_id": [1, 2, 3],
        "purchase_order_id": [1, 2, 3],
        "created_at": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"],
        "last_updated": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"]
    }

    df_test_transaction = pd.DataFrame(data)
    return df_test_transaction

@pytest.fixture
def transaction_missing_required_columns(transaction):
    df_missing_cols = transaction.drop(columns=["transaction_id"])
    return df_missing_cols

@pytest.fixture
def transaction_missing_unnecessary_columns(transaction):
    df_missing_cols = transaction.drop(columns=["last_updated"])
    return df_missing_cols

@pytest.fixture
def transaction_with_blanks():
    data = {
        "transaction_id": [1, 2, 3],
        "transaction_type": ["PURCHASE", "PURCHASE", "SALE"],
        "sales_order_id": [None, 2, 3],
        "purchase_order_id": [None, 2, 3],
        "created_at": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"],
        "last_updated": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"]
    }
    df_with_blanks = pd.DataFrame(data)
    return df_with_blanks

class TestTransaction:

    def test_returns_dataframe(self, transaction):
        output = util_dim_transaction(transaction)
        assert isinstance(output, pd.DataFrame)

    def test_returns_correct_columns(self, transaction):
        output = util_dim_transaction(transaction)
        assert list(output.columns) == [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id"]

    def test_returns_empty_values_where_acceptable(self, transaction_with_blanks):
        output = util_dim_transaction(transaction_with_blanks)

        assert pd.isnull(output["sales_order_id"].iloc[0])
        assert pd.isnull(output["purchase_order_id"].iloc[0])

    def test_returns_correct_columns_when_unneccesary_column_missing(self, transaction_missing_unnecessary_columns):
        output = util_dim_transaction(transaction_missing_unnecessary_columns)

        assert list(output.columns) == [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id"]

class TestTransactionErrors:

    def test_empty_dataframe(self):
        df_blank = pd.DataFrame()
        output = util_dim_transaction(df_blank)

        assert output == "Error: The source transaction dataframe is empty"

    def test_required_column_missing(self, transaction_missing_required_columns):
        output = util_dim_transaction(transaction_missing_required_columns)

        assert output == "Error: Missing columns transaction_id for the source transaction dataframe"