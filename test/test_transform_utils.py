import pytest
from moto import mock_aws
import pandas as pd
import boto3
import os
from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError, PartialCredentialsError
from unittest.mock import patch

from src.transform_utils.file_utils import read_csv_from_s3, write_parquet_to_s3

@pytest.fixture(scope="function",autouse=True)
def aws_credentials():
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
            
    def test_returns_no_credentials_error(self):
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.side_effect = NoCredentialsError()
            
            df = pd.DataFrame({'col1': [1, 2, 3]})
            result = write_parquet_to_s3(df, 'test-bucket', 'test-data.parquet')

            assert result == "AWS credentials not found or incomplete."
            
    def test_returns_no_such_key_error_message(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_key = 'test-data.csv'

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'})

            result = read_csv_from_s3(bucket_name, file_key)
            assert result == "The file 'test-data.csv' does not exist in the bucket 'test-bucket'."

    def test_returns_access_denied_error_message(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_key = 'test-data.csv'

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})

            csv_content = """payment_type_id,payment_type_name,created_at,last_updated\n
            1,SALES_RECEIPT,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000\n
            2,SALES_REFUND,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000"""

            s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content)

            with patch("boto3.client") as mock_boto_client:
                mock_s3 = mock_boto_client.return_value
                mock_s3.get_object.side_effect = ClientError(
                    {"Error": {"Code": "AccessDenied"}}, "GetObject"
                )

                result = read_csv_from_s3(bucket_name, file_key)
                assert result == f"Access denied to '{bucket_name}'. Check your S3 permissions."
            
class TestWriteParquetToCsv:

    def test_successful_upload_returns_success_string(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_key = 'test_parquet'

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})
            
            df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

            result = write_parquet_to_s3(df, bucket_name, file_key)
            assert result == f"File successfully uploaded to s3://{bucket_name}/{file_key}"

    def test_successful_write_to_s3_bucket(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_key = 'test.parquet'

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})
            
            df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

            write_parquet_to_s3(df, bucket_name, file_key)
            objects = s3.list_objects_v2(Bucket=bucket_name)
            keys = [item['Key'] for item in objects.get('Contents', [])]
            assert file_key in keys

    def test_returns_no_credentials_error(self):
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.side_effect = NoCredentialsError()
            
            df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
            result = write_parquet_to_s3(df, 'test-bucket', 'test.parquet')

            assert result == "AWS credentials not found or incomplete."

    def test_returns_botocore_error(self):
        with patch("boto3.client") as mock_boto_client:
                mock_boto_client.side_effect = BotoCoreError()

                df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
                result = write_parquet_to_s3(df, 'test-bucket', 'test.parquet')

                assert "An AWS error occurred" in result