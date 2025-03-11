from src.transform_utils.fact_payment import util_fact_payment
import pandas as pd
import pytest

@pytest.fixture
def payment():
    data = {
        "payment_id": [1, 2, 3],
        "created_at": ["2022-11-03 14:20:52.187", "2022-11-03 14:20:52.186", "2022-11-03 14:20:52.187"],
        "last_updated": ["2022-11-03 14:20:52.187", "2022-11-03 14:20:52.186", "2022-11-03 14:20:52.187"],
        "transaction_id": [1, 2, 3],
        "counterparty_id": [15, 18, 17],
        "payment_amount": [552548.62, 205952.22, 57067.20],
        "currency_id": [1, 2, 3],
        "payment_type_id": [1, 2, 3],
        "paid": [False, False, True],
        "payment_date": ["2022-11-04", "2022-11-04", "2022-11-04"],
        "company_ac_number": [67305075, 67305075, 67305075],
        "counterparty_ac_number": [47839086, 47839086, 47839086]
    }

    df_test = pd.DataFrame(data)
    return df_test

@pytest.fixture
def payment_missing_columns(payment):
    df_missing_cols = payment.drop(columns=["payment_id", "transaction_id"])
    return df_missing_cols

class TestFactPayment:

    def test_returns_dataframe(self, payment):
        output = util_fact_payment(payment)
        assert isinstance(output, pd.DataFrame)

    def test_returns_correct_columns(self, payment):
        output = util_fact_payment(payment)

        assert list(output.columns) == [
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date"]

class TestFactPaymentErrors:

    def test_empty_dataframe(self, payment):
        empty_df = pd.DataFrame()
        output = util_fact_payment(empty_df)
        assert output == "Error: The source dataframe for fact_payment is empty"

    def test_missing_columns(self, payment_missing_columns):
        output = util_fact_payment(payment_missing_columns)
        assert output == "Error: Missing columns payment_id, transaction_id"