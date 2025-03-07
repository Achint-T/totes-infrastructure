 # get csv files from ingestion bucket - async
# logic to take only new/updated files

# returns pd dataframe. Each dataframe is assigned a variable name df_tablename. 

# run fact_sales_orders util on dataframe - outputs new dataframe
# run all dimension utils (taking from fact dataframe)
# write to parquet util function on all resulting dataframes - async

# general client error handling

from src.transform_utils.file_utils import read_csv_from_s3, write_parquet_to_s3

from src.transform_utils.fact_sales_order import util_fact_sales_order
from src.transform_utils.fact_purchase_order import util_fact_purchase_order
from src.transform_utils.fact_payment import util_fact_payment

from src.transform_utils.dim_staff import util_dim_staff
from src.transform_utils.dim_counterparty import util_dim_counterparty
from src.transform_utils.dim_currency import util_dim_currency
from src.transform_utils.dim_date import util_dim_date
from src.transform_utils.dim_design import util_dim_design
from src.transform_utils.dim_location import util_dim_location

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, UTC
import logging
import os
import botocore

"""6/3/25 16:40 - ingestion lambda will now ingest entirety of each dim-to-be-table
every run, but only partial ingestion of sales_order and other fact-to-be-tables 
(ie only the updated rows of the table will be in the csv).

ingestion lambda will return a dictionary in the form:

 {'fact_tables':{'sales_order': '2025/6/3/16/47/sales_order.csv',...}, 
  'dim_tables':{'staff': '2025/6/3/16/47/staff', 'department': '2025/6/3/16/47/department', ...}}

  the transform lambda will take this as its EVENT and run transformation utils on all the
   dim tables, and on fact-to-be-tables e.g sales_order IF there are updates. (if there are
   no updates the dictionary value of the table will be None) """

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
ingestion_bucket = os.environ["INGESTION_BUCKET_NAME"]
transformed_bucket = os.environ["TRANSFORMED_BUCKET_NAME"]
# export INGESTION_BUCKET_NAME="fake-ingestion"
# export TRANSFORMED_BUCKET_NAME="mock-transformed"

def run_dim_utils(event, ingestion_bucket_name):
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
            dfs[table] = read_csv_from_s3(ingestion_bucket_name, event['dim_tables'][table])
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

def run_fact_utils(event, ingestion_bucket_name):
    """runs transformation utils on fact-to-be-tables (e.g sales_order --> fact_sales_order)"""
    
    transformed_dfs = {} 
    dfs = {}

    for table in event['fact_tables']:
        try:
            if event['fact_tables'][table]:
                dfs[table] = read_csv_from_s3(ingestion_bucket_name, event['fact_tables'][table])
            else:
                logger.info(f"No passed csv file for {table}")
        except:
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

def lambda_handler(event,context):
    #no need to pass bucket names do env vars
    """Event example:
     {'fact_tables':{'sales_order': '2025/6/3/16/47/sales_order.csv',...}, 
  'dim_tables':{'staff': '2025/6/3/16/47/staff', 'department': '2025/6/3/16/47/department', ...}}
    """
    ingestion_bucket = event.get("ingestion_bucket")
    transformed_bucket = event.get("transformed_bucket")
    tables_to_check = event.get("tables_to_check")
    new_tables = get_new_tables(ingestion_bucket, transformed_bucket)

    if not new_tables:
        print("no new tables to process")
        return {"message": "No updates found"}
    
    if not ingestion_bucket or not transformed_bucket or not tables_to_check:
        raise ValueError("Missing required parameters: ingestion_bucket, transformed_bucket, tables_to_check")

    try:
        #tables to write to s3:
        to_write = transform_where_new_tables(ingestion_bucket, transformed_bucket)
        #write tables to s3:    

        now = datetime.now(UTC)
        timestamp_path = f"{now.year}/{now.month:02}/{now.day:02}/{now.hour:02}/{now.minute:02}"

        for table, df in to_write.items():
            s3_key = f"{table}/{timestamp_path}/data.parquet"
            write_parquet_to_s3(df, transformed_bucket, s3_key)
            print(f"Written {table} to {s3_key}")

        return {
            "message": "Transformation complete",
            "transformed_tables": list(to_write.keys())
        }
    except Exception as e:
        logging.error(f"writing to transformed bucket failes: {e}")
        return {"statusCode": 500, "body": f"Transform run failed: {e}"}

def transform_where_new_tables(ingestion_bucket, transformed_bucket):
    """runs relevant transformation util (returning pandas dataframe) where a star schema table has new dependency tables"""

    #determine which star schema tables need to be updated (have new dependency table versions):

    new_tables = get_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed')
    tables_to_check = {dep for deps in table_relations.values() for dep in deps}
    latest_ingested_tables = get_latest_ingested_tables(ingestion_bucket,tables_to_check)
    
    s_tables_to_update = []

    for s_table in table_relations:
        if any(x in table_relations[s_table] for x in new_tables.keys()):
            s_tables_to_update.append(s_table)
            print(f'needs to be updated - new dependency tables found: {s_table}')

    #read the necessary dependency from the ingestion bucket to update star schema table:
    df_dict = {}
    loaded_tables = set()

    for s in s_tables_to_update:
        for table in table_relations[s]:
            if table in loaded_tables:
                continue

            key = None
            if table in new_tables:
                key = new_tables[table][0] 
            elif table in latest_ingested_tables:
                key = latest_ingested_tables[table][0]

            if key:
                df_dict[f"df_{table}"] = read_csv_from_s3(ingestion_bucket,key)
                loaded_tables.add(table)
                print(f"Loaded {table} from {key}")
    
    print(df_dict)
    df_transformed = {}

    if "df_sales_order" in df_dict:
        df_transformed["fact_sales_order"] = util_fact_sales_order(df_dict["df_sales_order"])

    if "df_staff" in df_dict and "df_department" in df_dict:
        df_transformed["dim_staff"] = util_dim_staff(df_dict["df_staff"], df_dict["df_department"])

    if "df_counterparty" in df_dict and "df_address" in df_dict:
        df_transformed["dim_counterparty"] = util_dim_counterparty(df_dict["df_counterparty"], df_dict["df_address"])

    if "df_design" in df_dict:
        df_transformed["dim_design"] = util_dim_design(df_dict["df_design"])

    if "df_address" in df_dict:
        df_transformed["dim_location"] = util_dim_location(df_dict["df_address"])

    print(f"Transformed tables: {list(df_transformed.keys())}")

    return df_transformed
