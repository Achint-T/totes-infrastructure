import pytest
import pandas as pd
import pg8000
from unittest.mock import MagicMock

from src.load_utils.replace_dataframe_to_dw import replace_dataframe_to_db 


class TestReplaceDataframeToDb:

    @pytest.fixture
    def mock_dataframe(self):
        return pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

    @pytest.fixture
    def empty_dataframe(self):
        return pd.DataFrame()

    @pytest.fixture
    def mock_conn(self):
        return MagicMock(spec=pg8000.Connection)

    def test_giving_invalid_dataframe_type_when_replace_dataframe_to_db_then_raise_typeerror(self, mock_conn):
        with pytest.raises(TypeError):
            replace_dataframe_to_db("not_a_dataframe", mock_conn, "test_table")

    def test_giving_invalid_conn_type_when_replace_dataframe_to_db_then_raise_typeerror(self, mock_dataframe):
        with pytest.raises(TypeError):
            replace_dataframe_to_db(mock_dataframe, "not_a_conn", "test_table")

    def test_giving_invalid_table_name_type_when_replace_dataframe_to_db_then_raise_typeerror(self, mock_dataframe, mock_conn):
        with pytest.raises(TypeError):
            replace_dataframe_to_db(mock_dataframe, mock_conn, 123)

    def test_giving_empty_dataframe_when_replace_dataframe_to_db_then_raise_valueerror(self, empty_dataframe, mock_conn):
        with pytest.raises(ValueError):
            replace_dataframe_to_db(empty_dataframe, mock_conn, "test_table")

    def test_giving_empty_table_name_when_replace_dataframe_to_db_then_raise_valueerror(self, mock_dataframe, mock_conn):
        with pytest.raises(ValueError):
            replace_dataframe_to_db(mock_dataframe, mock_conn, "")

    def test_giving_valid_dataframe_and_conn_when_replace_dataframe_to_db_then_return_true(self, mock_dataframe, mock_conn):
        mock_conn.run.return_value = None
        result = replace_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        assert result is True

    def test_giving_db_error_when_replace_dataframe_to_db_then_raise_pg8000_error(self, mock_dataframe, mock_conn):
        mock_conn.run.side_effect = pg8000.Error("Database error")
        with pytest.raises(pg8000.Error) as excinfo:
            replace_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        assert "Database error" in str(excinfo.value)

    def test_giving_valid_dataframe_when_replace_dataframe_to_db_then_not_modify_dataframe(self, mock_dataframe, mock_conn):
        dataframe_copy = mock_dataframe.copy()
        replace_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        pd.testing.assert_frame_equal(mock_dataframe, dataframe_copy)

    def test_giving_valid_dataframe_and_conn_when_replace_dataframe_to_db_then_execute_correct_sql(self, mock_dataframe, mock_conn):
        table_name = "test_table"
        replace_dataframe_to_db(mock_dataframe, mock_conn, table_name)
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