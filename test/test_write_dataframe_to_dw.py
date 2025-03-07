import pytest
import pandas as pd
import pg8000
from unittest.mock import MagicMock

from src.load_utils.write_dataframe_to_dw import write_dataframe_to_db  # Assuming your function is in src/task_name.py


class TestWriteDataframeToDb:
    @pytest.fixture
    def mock_dataframe(self):
        return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    @pytest.fixture
    def empty_dataframe(self):
        return pd.DataFrame()

    @pytest.fixture
    def mock_conn(self):
        return MagicMock(spec=pg8000.Connection)

    def test_giving_invalid_dataframe_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_conn
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db("not_a_dataframe", mock_conn, "test_table")

    def test_giving_invalid_conn_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_dataframe
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db(mock_dataframe, "not_a_conn", "test_table")

    def test_giving_invalid_table_name_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_dataframe, mock_conn
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db(mock_dataframe, mock_conn, 123)

    def test_giving_invalid_insert_mode_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_dataframe, mock_conn
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db(mock_dataframe, mock_conn, "test_table", insert_mode="not_a_bool")

    def test_giving_empty_dataframe_when_write_dataframe_to_db_then_raise_valueerror(
        self, empty_dataframe, mock_conn
    ):
        with pytest.raises(ValueError):
            write_dataframe_to_db(empty_dataframe, mock_conn, "test_table")

    def test_giving_empty_table_name_when_write_dataframe_to_db_then_raise_valueerror(
        self, mock_dataframe, mock_conn
    ):
        with pytest.raises(ValueError):
            write_dataframe_to_db(mock_dataframe, mock_conn, "")

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_insert_mode_then_return_true(
        self, mock_dataframe, mock_conn
    ):
        mock_conn.run.return_value = None
        result = write_dataframe_to_db(mock_dataframe, mock_conn, "test_table", insert_mode=True)
        assert result is True

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_replace_mode_then_return_true(
        self, mock_dataframe, mock_conn
    ):
        mock_conn.run.return_value = None
        result = write_dataframe_to_db(mock_dataframe, mock_conn, "test_table", insert_mode=False)
        assert result is True

    def test_giving_db_error_when_write_dataframe_to_db_then_raise_pg8000_error(
        self, mock_dataframe, mock_conn
    ):
        mock_conn.run.side_effect = pg8000.Error("Database error")
        with pytest.raises(pg8000.Error) as excinfo:
            write_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        assert "Database error" in str(excinfo.value)

    def test_giving_valid_dataframe_when_write_dataframe_to_db_then_not_modify_dataframe(
        self, mock_dataframe, mock_conn
    ):
        dataframe_copy = mock_dataframe.copy()
        write_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        pd.testing.assert_frame_equal(mock_dataframe, dataframe_copy)

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_insert_mode_then_execute_correct_insert_sql(
        self, mock_dataframe, mock_conn
    ):
        table_name = "test_table"
        write_dataframe_to_db(mock_dataframe, mock_conn, table_name, insert_mode=True)
        calls = mock_conn.run.call_args_list

        expected_sql_list = [
            f"INSERT INTO {table_name} (col1,col2) VALUES (%s,%s)",
            [[1, 'a'], [2, 'b']]
        ]
        assert len(calls) == 1
        assert calls[0].args[0] == expected_sql_list[0]
        assert calls[0].args[1] == expected_sql_list[1]

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_replace_mode_then_execute_correct_replace_sql(
        self, mock_dataframe, mock_conn
    ):
        table_name = "test_table"
        write_dataframe_to_db(mock_dataframe, mock_conn, table_name, insert_mode=False)
        calls = mock_conn.run.call_args_list

        expected_sql_list = [
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} (col1 INTEGER,col2 TEXT)",
            f"INSERT INTO {table_name} (col1,col2) VALUES (%s,%s)",
            [[1, 'a'], [2, 'b']]
        ]
        assert len(calls) == 3
        assert calls[0].args[0] == expected_sql_list[0]
        assert calls[1].args[0] == expected_sql_list[1]
        assert calls[2].args[0] == expected_sql_list[2]
        assert calls[2].args[1] == expected_sql_list[3]

    def test_giving_dataframe_with_different_dtypes_when_write_dataframe_to_db_in_replace_mode_then_create_table_with_correct_dtypes(self, mock_conn):
        table_name = "dtype_test_table"
        dtype_df = pd.DataFrame({
            "int_col": [1, 2],
            "float_col": [1.1, 2.2],
            "str_col": ["a", "b"],
            "date_col": pd.to_datetime(['2023-10-26', '2023-10-27'])
        })
        write_dataframe_to_db(dtype_df, mock_conn, table_name, insert_mode=False)
        calls = mock_conn.run.call_args_list

        expected_create_table_sql = f"CREATE TABLE {table_name} (int_col INTEGER,float_col FLOAT,str_col TEXT,date_col TIMESTAMP)"

        assert calls[1].args[0] == expected_create_table_sql
