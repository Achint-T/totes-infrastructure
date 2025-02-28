import pytest
import boto3
import time
import logging
from moto import mock_aws
from unittest.mock import patch, mock_open, MagicMock
from io import StringIO
import os

from src.lambda_ingest import lambda_handler, fetch_credentials, export_db_creds_to_env, create_connection, get_last_upload_date, get_recent_additions, data_to_csv, close_db_connection

@pytest.fixture(scope="function",autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture
def secret_client(aws_credentials):
    secret_client = boto3.client("secretsmanager")
    yield secret_client

@pytest.fixture
def s3_client(aws_credentials):
    s3_client = boto3.client("s3")
    yield s3_client
