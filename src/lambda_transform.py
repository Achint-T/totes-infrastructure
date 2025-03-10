from transform_utils.file_utils import read_csv_from_s3, write_parquet_to_s3

from transform_utils.fact_sales_order import util_fact_sales_order
from transform_utils.fact_purchase_order import util_fact_purchase_order
from transform_utils.fact_payment import util_fact_payment

from transform_utils.dim_staff import util_dim_staff
from transform_utils.dim_counterparty import util_dim_counterparty
from transform_utils.dim_currency import util_dim_currency
from transform_utils.dim_date import util_dim_date
from transform_utils.dim_design import util_dim_design
from transform_utils.dim_location import util_dim_location

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, UTC
import logging
import os
import json
# test tfstate 1223 10 mar

"""6/3/25 16:40 - Ingestion lambda will now ingest entirety of each dim-to-be-table
every run, but only partial ingestion of sales_order and other fact-to-be-tables 
(ie only the updated rows of the table will be in the csv).

Ingestion lambda will return a dictionary in the form:

{
"status_code": 200,
'fact_tables':{'sales_order': '2025/6/3/16/47/sales_order.csv',...}, 
'dim_tables':{'staff': '2025/6/3/16/47/staff', 'department': '2025/6/3/16/47/department', ...}
}

The transform lambda will take this as its EVENT and run transformation utils on all the
dim tables, and on fact-to-be-tables e.g sales_order IF there are updates. (if there are
no updates the fact table will not be present in event dict).

Returns dictionary in same format as that received from ingestion but with filepaths to
created parquet files in transformed bucket.
"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
ingestion_bucket = os.environ["INGESTION_BUCKET_NAME"]
transformed_bucket = os.environ["TRANSFORMED_BUCKET_NAME"]

def lambda_handler(event,context):
    """Event example:
    {'fact_tables':{'sales_order': '2025/6/3/16/47/sales_order.csv',...}, 
    'dim_tables':{'staff': '2025/6/3/16/47/staff', 'department': '2025/6/3/16/47/department', ...}}

    Returns dictionary of same format of event with filepaths to parquet files.

    Context not needed for this function. Can be anything
    '"""
    # logger.info(f"Received event: {event}")
    # if not isinstance(event, dict):
    #     raise TypeError("event must be a dictionary")
    # if "fact_tables" not in event:
    #     raise ValueError('event must contain "fact_tables"')
    # if "dim_tables" not in event:
    #     raise ValueError('event must contain "dim_tables"')
    #

    try:
        dim_table_dfs = run_dim_utils(event, ingestion_bucket)
        fact_table_dfs = run_fact_utils(event, ingestion_bucket)
            
        now = datetime.now(UTC)
        timestamp_path = f"{now.year}/{now.month:02}/{now.day:02}/{now.hour:02}/{now.minute:02}"

        fact_tables = {}
        dim_tables = {}

        for table, df in fact_table_dfs.items():
            s3_key = f"{timestamp_path}/{table}.parquet"
            write_parquet_to_s3(df, transformed_bucket, s3_key)
            fact_tables[table] = s3_key
            logger.info(f"Written {table} to {s3_key}")

        for table, df in dim_table_dfs.items():
            s3_key = f"{timestamp_path}/{table}.parquet"
            write_parquet_to_s3(df, transformed_bucket, s3_key)
            dim_tables[table] = s3_key
            logger.info(f"Written {table} to {s3_key}")

        return {
            "status_code": 200,
            "fact_tables": fact_tables,
            "dim_tables": dim_tables
            }
    except KeyError as ke:
        logging.error(f"missing key in event: {ke}")
        return {"status_code": 400, "body": f"Missing table information: {ke}"}
    except Exception as e:
        logging.error(f"writing to transformed bucket failed: {e}")
        return {"statusCode": 500, "body": f"Transform run failed: {e}"}

def run_dim_utils(event, ingestion_bucket):
    """runs dim utils for each of the passed dim_tables from the event. returns transformed dataframes"""
    table_relations = {'fact_sales_order': ['sales_order'],
                'dim_staff': ['staff','department'],
                'dim_counterparty': ['counterparty', 'address'],
                'dim_currency': ['currency'],
                'dim_date': ['date'],
                'dim_design': ['design'],
                'dim_location': ['address']}
    
    transformed_dfs = {} 
    dfs = {}
    
    for table in event['dim_tables']:
        try:
            dfs[table] = read_csv_from_s3(ingestion_bucket, event['dim_tables'][table])
        except Exception as e:
            logger.error(f"Failed to read {table} from S3: {e}")
            continue  

    for dim_table, required_tables in table_relations.items():
        try:
            if all(tbl in dfs for tbl in required_tables):
                if dim_table == 'dim_staff':
                    transformed_dfs[dim_table] = util_dim_staff(dfs['staff'], dfs['department'])
                elif dim_table == 'dim_counterparty':
                    transformed_dfs[dim_table] = util_dim_counterparty(dfs['counterparty'], dfs['address'])
                elif dim_table == 'dim_currency':
                    transformed_dfs[dim_table] = util_dim_currency(dfs['currency'])
                elif dim_table == 'dim_location':
                    transformed_dfs[dim_table] = util_dim_location(dfs['address'])
                elif dim_table == 'dim_date':
                    transformed_dfs[dim_table] = util_dim_date(dfs['date'])
                elif dim_table == 'dim_design':
                    transformed_dfs[dim_table] = util_dim_design(dfs['design'])

                logger.info(f"successfully transformed {dim_table}")
            else:
                logger.info(f"could not construct {dim_table}. Missing one of {required_tables}")
        except Exception as e:
                logger.error(f"Error processing {dim_table}: {e}")

    return transformed_dfs

def run_fact_utils(event, ingestion_bucket):
    """runs transformation utils on fact-to-be-tables (e.g sales_order --> fact_sales_order)"""
    
    transformed_dfs = {} 
    dfs = {}

    for table in event['fact_tables']:
        try:
            if event['fact_tables'][table]:
                dfs[table] = read_csv_from_s3(ingestion_bucket, event['fact_tables'][table])
            else:
                logger.info(f"No passed csv file for {table}")
        except Exception as e:
            logger.error(f"Failed to read {table} from S3: {e}")
            continue  

    for df in dfs:
        try:
            if df == 'sales_order':
                transformed_dfs[f'fact_{df}'] = util_fact_sales_order(dfs['sales_order'])
            elif df == 'purchase_order':
                transformed_dfs[f'fact_{df}'] = util_fact_purchase_order(dfs['purchase_order'])
            elif df == 'payment':
                transformed_dfs[f'fact_{df}'] =  util_fact_payment(dfs['payment'])
            logger.info(f"successfully transformed {df}")

        except Exception as e:
            logger.error(f"Error processing {df}: {e}")

    return transformed_dfs


# event =     {
#   "status_code": 200,
#   "fact_tables": {
#     "sales_order": "2025/03/06/22/51/sales_order.csv"
#   },
#   "dim_tables": {
#     "design": "2025/03/06/22/51/design.csv",
#     "currency": "2025/03/06/22/51/currency.csv",
#     "staff": "2025/03/06/22/51/staff.csv",
#     "counterparty": "2025/03/06/22/51/counterparty.csv",
#     "address": "2025/03/06/22/51/address.csv",
#     "department": "2025/03/06/22/51/department.csv",
#     "transaction": "2025/03/06/22/51/transaction.csv",
#     "payment_type": "2025/03/06/22/51/payment_type.csv"
#   }
# }
# lambda_handler(event,{})
