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
def transaction_missing_columns(transaction):
    df_missing_cols = transaction.drop(columns=["transaction_id"])
    return df_missing_cols

@pytest.fixture
def transaction_with_blanks(transaction):
    data = {
        "transaction_id": [1, 2, 3],
        "transaction_type": ["PURCHASE", "PURCHASE", "SALE"],
        "sales_order_id": [None, None, 3],
        "purchase_order_id": [1, 2, None],
        "created_at": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"],
        "last_updated": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"]
    }
    df_with_blanks = pd.DataFrame(data)
    return df_with_blanks