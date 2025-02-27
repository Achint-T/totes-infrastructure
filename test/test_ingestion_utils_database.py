import pytest
import os
from unittest.mock import MagicMock 
from src.ingestion_utils.database_utils import create_connection, get_recent_additions

class TestCreateConnection:

    def test_giving_missing_username_env_when_create_connection_then_raises_keyerror(self):
        os.environ['USERNAME'] = 'testusername'
        os.environ['HOST'] = 'testhost'
        os.environ['PASSWORD'] = '1234'
        os.environ['DBNAME'] = 'testdb'

        with pytest.raises(KeyError) as excinfo:
            create_connection()

        assert "PORT" in str(excinfo.value)
        assert "Missing environment variable:" in str(excinfo.value)
        # Cleanup environment variables
        del os.environ['USERNAME']
        del os.environ['HOST']
        del os.environ['PASSWORD']
        del os.environ['DBNAME']

class TestGetRecentAdditions:
    def test_giving_valid_connection_table_updatedate_time_now_when_get_recent_additions_then_returns_data(self):
        mock_conn = MagicMock()
        mock_conn.run.return_value = [['row1_col1', 'row1_col2'], ['row2_col1', 'row2_col2']]
        mock_conn.columns = [{'name': 'col1'}, {'name': 'col2'}]
        table_name = 'test_table'
        update_date = '2023-01-01 00:00:00'
        time_now = '2023-01-02 00:00:00'

        result = get_recent_additions(mock_conn, table_name, update_date, time_now)

        assert isinstance(result, dict)
        assert 'headers' in result
        assert 'body' in result
        assert result['headers'] == ['col1', 'col2']
        assert result['body'] == [['row1_col1', 'row1_col2'], ['row2_col1', 'row2_col2']]
        mock_conn.run.assert_called_once_with(f'SELECT * FROM {table_name} WHERE last_updated BETWEEN \'{update_date}\' AND \'{time_now}\';')


    def test_giving_connection_error_when_get_recent_additions_then_raises_exception(self):
        mock_conn = MagicMock()
        mock_conn.run.side_effect = Exception("Database error")
        mock_conn.columns = [{'name': 'col1'}] 
        table_name = 'test_table'
        update_date = '2023-01-01 00:00:00'
        time_now = '2023-01-02 00:00:00'

        with pytest.raises(Exception) as excinfo:
            get_recent_additions(mock_conn, table_name, update_date, time_now)

        assert "Error fetching recent additions" in str(excinfo.value)
        mock_conn.run.assert_called_once_with(f'SELECT * FROM {table_name} WHERE last_updated BETWEEN \'{update_date}\' AND \'{time_now}\';')

    