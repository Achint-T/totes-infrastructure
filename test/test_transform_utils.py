import pytest
from moto import mock_aws
import pandas as pd
import boto3
import os

from src.transform_utils.file_utils import read_csv_from_s3

@pytest.fixture(scope="function",autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestReadCsvFromS3:

    def test_returns_dataframe(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_key = 'test-data.csv'

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
            csv_content = """payment_type_id,payment_type_name, created_at, last_updated\n
            1,SALES_RECEIPT, 2022-11-03 14:20:49.962000, 2022-11-03 14:20:49.962000\n
            2, SALES_REFUND, 2022-11-03 14:20:49.962000, 2022-11-03 14:20:49.962000"""

            s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content)

            result = read_csv_from_s3(bucket_name, file_key)

            assert isinstance(result, pd.DataFrame)

    def test_returns_correct_columns(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_key = 'test-data.csv'

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
            csv_content = """payment_type_id,payment_type_name, created_at, last_updated\n
            1,SALES_RECEIPT, 2022-11-03 14:20:49.962000, 2022-11-03 14:20:49.962000\n
            2, SALES_REFUND, 2022-11-03 14:20:49.962000, 2022-11-03 14:20:49.962000"""

            s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content)

            result = read_csv_from_s3(bucket_name, file_key)

            assert list(result.columns) == ['payment_type_id',
                                            'payment_type_name',
                                            ' created_at',
                                            ' last_updated']