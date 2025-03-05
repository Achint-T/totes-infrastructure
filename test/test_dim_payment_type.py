from src.transform_utils.dim_payment_type import util_dim_payment_type
import pandas as pd
import pytest

@pytest.fixture
def payment_type():
    data = {
        "payment_type_id": [1, 2, 3],
        "payment_type_name": ["SALES_RECEIPT", "SALES_RECEIPT", "SALES_RECEIPT"],
        "created_at": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"],
        "last_updated": ["2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186"]
    }
    
    df_payment = pd.DataFrame(data)
    return df_payment

@pytest.fixture
def payment_missing_unnecessary_columns(payment_type):
    df_missing_unnecessary_columns = payment_type.drop(columns=["created_at"])
    return df_missing_unnecessary_columns

@pytest.fixture
def payment_missing_required_columns(payment_type):
    df_missing_required_columns = payment_type.drop(columns=["payment_type_id"])
    return df_missing_required_columns