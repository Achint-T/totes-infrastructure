from src.lambda_transform import lambda_handler, run_dim_utils, run_fact_utils
from src.transform_utils.file_utils import read_csv_from_s3
import os
import boto3
import pytest
from moto import mock_aws
import pandas as pd
import io


"""to test lambda_handler trialled using 'fake-ingestion' and 'mock-transform' 
all working fine"""

@pytest.fixture(scope="function",autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestRunDimUtils:
    def test_runs_right_utils(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_keys = {
                "staff": "2025/3/5/16/50/staff.csv", "department":
                "2025/3/5/16/50/department.csv"
            }

            event = {
                'fact_tables':{'sales_order': '2025/6/3/16/47/sales_order.csv'}, 
    'dim_tables':{'staff': "2025/3/5/16/50/staff.csv", "department": "2025/3/5/16/50/department.csv"}
                    }
            
            
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'})

            df_staff = pd.DataFrame([
                [1, "Jeremie", "Franey", "jeremie.franey@terrifictotes.com", 101],
                [2, "Deron", "Beier", "deron.beier@terrifictotes.com", 102],
                [3, "Jeanette", "Erdman", "jeanette.erdman@terrifictotes.com", 102],
                [4, "Ana", "Glover", "ana.glover@terrifictotes.com", 103]
            ], columns=["staff_id", "first_name", "last_name", "email_address", "department_id"])

            df_department = pd.DataFrame([
                [101, "Purchasing", "Manchester"],
                [102, "Facilities", "Manchester"],
                [103, "Production", "Leeds"]
            ], columns=["department_id", "department_name", "location"])

            for table, file_key in file_keys.items():
                csv_buffer = io.StringIO()
                if table == "staff":
                    df_staff.to_csv(csv_buffer, index=False)
                else:
                    df_department.to_csv(csv_buffer, index=False)
                s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

            result = run_dim_utils(event, bucket_name)

            assert isinstance(result, dict)
            assert "dim_staff" in result
            assert isinstance(result["dim_staff"], pd.DataFrame)
    
class TestRunFactUtils:
    def test_runs_right_fact_utils(self):
        with mock_aws():
            s3 = boto3.client('s3')
            bucket_name = 'test-bucket'
            file_keys = {
                'sales_order': '2025/6/3/16/47/sales_order.csv'
            }

            event = {
                'fact_tables':{'sales_order': '2025/6/3/16/47/sales_order.csv'}}
            
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'})

            df_sales_order = pd.DataFrame([
            [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2], 
            [3, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 5, 2, 4, 2317, 3.94, 3, '2022-11-07', '2022-11-08', 2]
            ], columns=['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold',
                         'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
        )

            for table, file_key in file_keys.items():
                csv_buffer = io.StringIO()
                if table == "sales_order":
                    df_sales_order.to_csv(csv_buffer, index=False)
                s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

            result = run_fact_utils(event, bucket_name)

            assert isinstance(result, dict)
            assert "fact_sales_order" in result
            assert isinstance(result["fact_sales_order"], pd.DataFrame)

    def test_run_dim_utils_handles_None_value_in_event(self):
        with mock_aws():
            s3 = boto3.client("s3")
            bucket_name = "test-bucket"

            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2"
            })
            event = {
                "fact_tables": {
                }
            }
            result = run_fact_utils(event, bucket_name)
            assert "fact_sales_order" not in result

