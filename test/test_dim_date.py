from src.transform_utils.dim_date import *
import pandas as pd

class TestDimDate:

    def test_returns_data_frame(self):
        output = util_dim_date('2022-01-01', '2022-01-31')
        assert isinstance(output, pd.DataFrame)

    def test_returns_correct_columns(self):
        output = util_dim_date('2022-01-01', '2022-01-31')

        assert list(output.columns) == ["date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter"]

    def test_returns_correctly_formatted_values(self):
        output = util_dim_date('2022-01-01', '2022-01-31')
        assert output['date_id'].iloc[0] == pd.Timestamp('2022-01-01')
        assert output['year'].iloc[0] == 2022
        assert output['day_name'].iloc[0] == 'Saturday'