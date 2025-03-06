from src.transform_utils.get_latest_utils import get_latest_ingested_tables, get_latest_transformed_tables,get_new_tables,extract_timestamp_from_filename
from datetime import datetime
import os
import boto3
import pytest
from moto import mock_aws
from unittest.mock import patch

#ingestion bucket empty
#check if each 'table to check' exists in ingestion bucket. log if not.
#get latest version of each ingestion table when there's multiple versions

#test util returns dictionary w/ names of latest table versions
#check correct logging messages returned (caplog)

def test_extract_timestamp_from_filename():
    assert extract_timestamp_from_filename('2025/3/5/17/0/address.csv') == datetime(2025, 3, 5, 17, 0)

@pytest.fixture(scope="function",autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestLatestIngestedTables:

    def test_returns_dictionary_with_correct_keys(self):
        """test returns dictionary w/ keys of inputted 'tables_to_check"""
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_keys = [
        "2025/3/5/17/0/address.csv",
        "2025/3/5/17/0/counterparty.csv",
        "2025/3/5/17/0/currency.csv",
        "2025/3/5/17/0/department.csv",
        "2025/3/5/17/0/design.csv",
        "2025/3/6/13/50/sales_order.csv",
        "2025/3/5/17/0/staff.csv"]
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})

        tables_to_check = ["sales_order", "staff", "design", "counterparty", "date", "address", "department"]
        for file_key in file_keys:
            s3.put_object(Bucket=bucket_name, Key=file_key, Body="")

        result = get_latest_ingested_tables(bucket_name,tables_to_check)

        assert isinstance(result, dict)
        





        