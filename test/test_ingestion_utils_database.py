import pytest
import boto3
import os
from unittest.mock import MagicMock 
from src.ingestion_utils.database_utils import create_connection, get_recent_additions, get_last_upload_date, get_current_time
from moto import mock_aws

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture()
def secrets_client(aws_credentials):
    with mock_aws():
        secrets_client = boto3.client("secretsmanager")
        yield secrets_client

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

class TestGetLastUploadDate:
    def test_gets_last_date_if_exists(
        self, secrets_client
    ):
        secret_name = "lastupload"
        secrets_client.create_secret(
            Name=secret_name,
            SecretString="1999-04-30 14:56:09")
        
        credentials = get_last_upload_date(secrets_client)
        assert isinstance(credentials, str)

    def test_giving_non_existing_secret_returns_default(
        self, secrets_client
    ):
        credentials = get_last_upload_date(secrets_client)

        assert credentials == '2020-01-01 00:00:00'

    def test_raises_exception_when_theres_an_error(self, secrets_client):

        with pytest.raises(Exception) as error:
            get_last_upload_date('hi')

        assert "Error fetching last upload" in str(error.value)

class TestGetLastUploadDate:
    def test_gets_last_date_if_exists(
        self, secrets_client
    ):
        secret_name = "lastupload"
        secrets_client.create_secret(
            Name=secret_name,
            SecretString="1999-04-30 14:56:09")
        
        credentials = get_last_upload_date(secrets_client)
        assert isinstance(credentials, str)

    def test_giving_non_existing_secret_returns_default(
        self, secrets_client
    ):
        credentials = get_last_upload_date(secrets_client)

        assert credentials == '2020-01-01 00:00:00'

    def test_raises_exception_when_theres_an_error(self, secrets_client):

        with pytest.raises(Exception) as error:
            get_last_upload_date('hi')

        assert "Error fetching last upload" in str(error.value)

class TestGetCurrentTime:
    def test_converts_a_timestamp(self):
        expected_output = {'secret':'2024-03-14 16:07:31', 'filepath':'2024/3/14/16/7'}
        input = [2024,3,14,16,7,32]
        output = get_current_time(input)
        assert output == expected_output

    def test_converts_a_timestamp_second(self):
        expected_output = {'secret':'2021-04-15 03:17:52', 'filepath':'2021/4/15/3/17'}
        input = [2021,4,15,3,17,53]
        output = get_current_time(input)
        assert output == expected_output