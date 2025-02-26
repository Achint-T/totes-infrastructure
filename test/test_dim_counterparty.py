from src.transform_utils.dim_counterparty import *
from src.transform_utils.fact_sales_order import *
import pandas as pd
import pytest


@pytest.fixture
def fact_sales_order():
    data = [
        [
            1,
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            5,
            2,
            2,
            2317,
            3.94,
            3,
            "2022-11-07",
            "2022-11-08",
            2,
        ],
        [
            2,
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            5,
            2,
            4,
            2317,
            3.94,
            3,
            "2022-11-07",
            "2022-11-08",
            2,
        ],
        [
            3,
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            5,
            2,
            1,
            2317,
            3.94,
            3,
            "2022-11-07",
            "2022-11-08",
            2,
        ],
    ]

    test_df = pd.DataFrame(
        data,
        columns=[
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ],
    )

    df_fact_sales_order = util_fact_sales_order(test_df)
    return df_fact_sales_order

@pytest.fixture
def counterparty():
    data = {
        "counterparty_id": [1, 2, 3],
        "counterparty_legal_name": ["Name A", "Name B", "Name C"],
        "legal_address_id": [1, 2, 3],
        "commercial_contact": ["Contact 1", "Contact 2", "Contact 3"],
        "delivery_contact": ["Person", "Another person", "Yet another person"],
        "created_at": [
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
        "last_updated": [
            "2022-11-03 15:20:52.186",
            "2022-11-03 15:20:52.186",
            "2022-11-03 15:20:52.186",
        ],
    }
    df_counterparty = pd.DataFrame(data)
    return df_counterparty

@pytest.fixture
def address():
    data = [
        [
            1,
            "Address Line 1",
            "Address Line 2",
            "District 1",
            "A big city",
            "SW1 9DA",
            "United Kingdom",
            "07111111111",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
        [
            2,
            "A nice address",
            "the second line of the address",
            "District 1001",
            "Underwater city",
            "777888",
            "Atlantis",
            "182763892137",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
        [
            3,
            "House 1",
            "Unit 2",
            "District 3",
            "City 4",
            "567890",
            "Secret country",
            "122112211221",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
    ]
    df_address = pd.DataFrame(
        data,
        columns=[
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
        ],
    )
    return df_address

@pytest.fixture
def address_with_blanks():
    data = [
        [
            1,
            "Address Line 1",
            None,
            None,
            "A big city",
            "SW1 9DA",
            "United Kingdom",
            "07111111111",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
        [
            2,
            "A nice address",
            "the second line of the address",
            "District 1001",
            "Underwater city",
            "777888",
            "Atlantis",
            "182763892137",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
        [
            3,
            "House 1",
            "Unit 2",
            "District 3",
            "City 4",
            "567890",
            "Secret country",
            "122112211221",
            "2022-11-03 14:20:52.186",
            "2022-11-03 14:20:52.186",
        ],
    ]
    df_address_with_blanks = pd.DataFrame(
        data,
        columns=[
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
        ],
    )
    return df_address_with_blanks

@pytest.fixture
def address_with_missing_cols():
    data = [
        [
            1,
            "Address Line 1",
            "Address Line 2",
            "District 1",
            "A big city",
            "SW1 9DA",
            "United Kingdom",
            "07111111111",
            "2022-11-03 14:20:52.186",
        ]
    ]
    df_address = pd.DataFrame(
        data,
        columns=[
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
            "created_at",
        ],
    )
    return df_address


class TestSalesOrder:

    def test_returns_dataframe(self, fact_sales_order, counterparty, address):
        output = util_dim_counterparty(fact_sales_order, counterparty, address)
        assert isinstance(output, pd.DataFrame)

    def test_returns_correct_columns(self, fact_sales_order, counterparty, address):
        output = util_dim_counterparty(fact_sales_order, counterparty, address)
        assert list(output.columns) == [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

    def test_returns_empty_values_where_acceptable(self, fact_sales_order, counterparty, address_with_blanks):
        output = util_dim_counterparty(fact_sales_order, counterparty, address_with_blanks)

        assert output['counterparty_legal_address_line_2'].iloc[0] == None
        assert output['counterparty_legal_district'].iloc[0] == None


class TestSalesOrderErrorHandling:

    def test_empty_dataframe(self, counterparty, address):
        test_df = pd.DataFrame()
        output = util_dim_counterparty(test_df, counterparty, address)
        assert output == "Error: One or more of the source dataframes is empty"

    def test_missing_columns(self, fact_sales_order, counterparty, address_with_missing_cols):

        output = util_dim_counterparty(fact_sales_order, counterparty, address_with_missing_cols)
        assert output == "Error: Missing columns last_updated"


