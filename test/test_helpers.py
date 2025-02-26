import pytest
import os
import boto3
from unittest import mock
from moto import mock_aws
from src.helpers import fetch_credentials, export_db_creds_to_env


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


class TestFetchCredentials:
    def test_giving_credentials_exists_in_secrets_manager_when_fetch_credentials_return_expected_dict(
        self, secrets_client
    ):
        secret_name = "database"
        secrets_client.create_secret(
            Name=secret_name,
            SecretString=str(
                {
                    "cohort_id": "de_2024_12_02",
                    "user": "project_team_10",
                    "password": "password",
                    "host": "host",
                    "database": "database",
                    "port": 5432,
                }
            ),
        )
        credentials = fetch_credentials(secrets_client, secret_name="database")

        assert isinstance(credentials, dict)

    def test_giving_non_existing_secret_when_fetch_credentials_then_raises_error_and_logs(
        self, secrets_client
    ):
        with pytest.raises(Exception):
            secret_name = "database"
            fetch_credentials(secrets_client, secret_name)


# Helper function to clear environment variables before and after tests
def clear_env_vars(keys):
    for key in keys:
        if key in os.environ:
            del os.environ[key]


class TestExportDbCredsToEnv:
    test_env_keys = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST"]

    def setup_method(self):
        clear_env_vars(self.test_env_keys)

    def teardown_method(self):
        clear_env_vars(self.test_env_keys)

    def test_given_creds_not_dict_raises_ValueError(self):
        """Test: Creds not dict, raises ValueError."""
        invalid_credentials = "not a dictionary"
        expected_keys = ["username"]
        with pytest.raises(ValueError) as excinfo:
            export_db_creds_to_env(invalid_credentials, expected_keys)
        assert "Input credentials must be a dictionary." in str(excinfo.value)

    def test_given_missing_expected_key_raises_ValueError(self):
        """Test: Missing expected key, raises ValueError."""
        credentials_missing_key = {
            "username": "test_user",
            "password": "test_password",
            # 'host' key is missing
        }
        expected_keys = ["username", "password", "host"]
        with pytest.raises(ValueError) as excinfo:
            export_db_creds_to_env(credentials_missing_key, expected_keys)
        assert "Expected key 'host' not found in credentials dictionary." in str(
            excinfo.value
        )

    def test_given_value_is_none_raises_ValueError(self):
        """Test: Value is None, raises ValueError."""
        credentials_none_value = {
            "username": "test_user",
            "password": None,
            "host": "localhost",
        }
        expected_keys = ["username", "password", "host"]
        with pytest.raises(ValueError) as excinfo:
            export_db_creds_to_env(credentials_none_value, expected_keys)
        assert (
            "Value for key 'password' is None, which is not allowed for environment variables."
            in str(excinfo.value)
        )

    def test_given_empty_creds_dict_exports_no_vars(self):
        """Test: Empty creds dict, exports no vars."""
        empty_credentials = {}
        expected_keys = []
        export_db_creds_to_env(empty_credentials, expected_keys)
        assert not os.environ.get("DB_USERNAME")

    def test_given_valid_creds_exports_env_vars(self):
        """Test: Valid creds, exports env vars."""
        valid_credentials = {
            "username": "test_user",
            "password": "test_password",
            "host": "localhost",
        }
        expected_keys = ["username", "password", "host"]
        export_db_creds_to_env(valid_credentials, expected_keys)

        assert os.environ.get("USERNAME") == "test_user"
        assert os.environ.get("PASSWORD") == "test_password"
        assert os.environ.get("HOST") == "localhost"

    def test_given_extra_keys_valid_export_all(self):
        """Test: Extra keys in creds, valid export all."""
        credentials_extra_keys = {
            "username": "test_user",
            "password": "test_password",
            "host": "localhost",
            "extra_key": "extra_value",
        }
        expected_keys = [
            "username",
            "password",
            "host",
        ]
        export_db_creds_to_env(credentials_extra_keys, expected_keys)

        assert os.environ.get("USERNAME") == "test_user"
        assert os.environ.get("PASSWORD") == "test_password"
        assert os.environ.get("HOST") == "localhost"
        assert os.environ.get("EXTRA_KEY") == "extra_value"
