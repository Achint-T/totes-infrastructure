import pytest
import pandas as pd
from src.load_utils.write_dataframe_to_dw import construct_sql

class TestConstructSQL:
    @pytest.fixture
    def sample_dataframe(self):
        return pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.1, 2.2, 3.3]
        })

    @pytest.fixture
    def empty_dataframe(self):
        return pd.DataFrame({'col1': [], 'col2': [], 'col3': []})

    @pytest.fixture
    def dataframe_with_special_chars_in_columns(self):
        return pd.DataFrame({
            'col 1': [1, 2],
            'col-2': ['a', 'b'],
            'col.3': [True, False]
        })

    @pytest.fixture
    def dataframe_with_different_dtypes(self):
        return pd.DataFrame({
            'int_col': [1, 2],
            'str_col': ['a', 'b'],
            'float_col': [1.1, 2.2],
            'bool_col': [True, False],
            'none_col': [None, None]
        })
    
    @pytest.fixture
    def dataframe_with_date(self):
        return pd.DataFrame({
            "date_id": ["2025-1-26", "2024-12-26"],
            "year": [2025,2024],
            "month": [1, 12]
        })

    def test_giving_empty_dataframe_when_construct_sql_then_return_empty_insert_sql(self, empty_dataframe):
        table_name = "empty_table"
        expected_sql = "INSERT INTO empty_table (col1, col2, col3) VALUES ();"
        actual_sql = construct_sql(empty_dataframe, table_name, upsert=False)
        assert actual_sql == expected_sql

    def test_giving_dataframe_when_construct_sql_then_dataframe_is_not_modified(self, sample_dataframe):
        original_dataframe = sample_dataframe.copy()
        construct_sql(sample_dataframe, "test_table")
        pd.testing.assert_frame_equal(sample_dataframe, original_dataframe)

    def test_giving_dataframe_and_table_name_when_upsert_is_false_then_return_insert_sql(self, sample_dataframe):
        table_name = "test_table"
        expected_sql = "INSERT INTO test_table (col1, col2, col3) VALUES (1, 'a', 1.1), (2, 'b', 2.2), (3, 'c', 3.3);"
        actual_sql = construct_sql(sample_dataframe, table_name, upsert=False)
        assert actual_sql == expected_sql

    def test_giving_dataframe_and_table_name_when_upsert_is_true_then_return_upsert_sql(self, sample_dataframe):
        table_name = "test_table"
        expected_sql = """INSERT INTO test_table (col1, col2, col3) VALUES (1, 'a', 1.1), (2, 'b', 2.2), (3, 'c', 3.3)
ON CONFLICT (col1) DO UPDATE SET col2=excluded.col2, col3=excluded.col3;"""
        actual_sql = construct_sql(sample_dataframe, table_name, upsert=True)
        assert actual_sql == expected_sql

    def test_giving_dataframe_with_special_chars_in_columns_when_construct_sql_then_return_escaped_column_names_sql(self, dataframe_with_special_chars_in_columns):
        table_name = "special_chars_table"
        expected_sql = "INSERT INTO special_chars_table (\"col 1\", \"col-2\", \"col.3\") VALUES (1, 'a', TRUE), (2, 'b', FALSE);"
        actual_sql = construct_sql(dataframe_with_special_chars_in_columns, table_name, upsert=False)
        assert actual_sql == expected_sql

    def test_giving_dataframe_with_different_dtypes_when_construct_sql_then_return_correct_sql(self, dataframe_with_different_dtypes):
        table_name = "dtypes_table"
        expected_sql = "INSERT INTO dtypes_table (int_col, str_col, float_col, bool_col, none_col) VALUES (1, 'a', 1.1, TRUE, NULL), (2, 'b', 2.2, FALSE, NULL);"
        actual_sql = construct_sql(dataframe_with_different_dtypes, table_name, upsert=False)
        assert actual_sql == expected_sql
    
    def test_giving_dataframe_contains_date_when_construct_sql_then_return_correct_sql(self, dataframe_with_date):
        table_name = "dim_date"
        excepted_sql = "INSERT INTO dim_date (date_id, year, month) VALUES ('2025-1-26', 2025, 1), ('2024-12-26', 2024, 12);"
        actual_sql = construct_sql(dataframe_with_date, table_name, upsert=False)
        assert excepted_sql == actual_sql
