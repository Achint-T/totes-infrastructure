import boto3
from helpers import fetch_credentials, export_db_creds_to_env
from ingestion_utils.file_utils import data_to_csv, get_current_time
from ingestion_utils.database_utils import create_connection, close_db_connection, get_recent_additions, get_last_upload_date, put_last_upload_date
import time
import logging
import os

secret_client =  boto3.client("secretsmanager")
s3_client = boto3.client("s3")
bucket_name = os.environ["BUCKET_NAME"]

def lambda_handler(event, context):
    """Handles the lambda function invocation.

    Extracts data from specified tables, saves it to CSV, and uploads to S3.

    Args:
        event (dict): Event data passed to the lambda function. Must contain 'tables' key.
        context (object): Lambda context object (not used in this function).

    Returns:
        dict: Status code and message indicating success or failure.
    """
    conn = None
    try:
        tables_to_ingest = event["tables"]
        conn = get_connection()
        last_date = get_last_run_date()
        time_now = time.gmtime()
        timestamp = get_current_time(time_now)
        save_data_to_s3(conn, tables_to_ingest, last_date, timestamp)
        put_last_run_date(time_now)

        logging.info(f"Extraction run successfully for tables: {tables_to_ingest}")
        return {"statusCode": 200, "body": "Extraction run successfully"}

    except KeyError as ke:
        logging.error(f"Missing key in event: {ke}")
        #Reconsider suitable status code
        return {"statusCode": 400, "body": f"Missing table information: {ke}"}
    except Exception as e:
        logging.error(f"Extraction run failed: {e}")
        return {"statusCode": 500, "body": f"Extraction run failed: {e}"} 
    finally:
        if conn:
            close_db_connection(conn)

def get_connection(secret_client = secret_client):
    db_creds = fetch_credentials(secret_client, secret_name="database_credentials")
    export_db_creds_to_env(db_creds, ["username","password","port","host"])
    conn = create_connection()
    return conn

def get_last_run_date(secret_client = secret_client):
    return get_last_upload_date(secret_client)

def put_last_run_date(timeobject, secret_client = secret_client):
    return put_last_upload_date(timeobject, secret_client)

def save_data_to_s3(conn, tables_to_ingest,last_date, timestamp, s3_client=s3_client,):
    for table in tables_to_ingest:
        data = get_recent_additions(conn, tablename=table, updatedate=last_date, time_now=timestamp["secret"])
        if data["body"]:#check if there is actual data 
            data_to_csv(data, table_name=table)
            s3_client.upload_file(f"/tmp/{table}.csv", bucket_name, timestamp["filepath"] + '/' + table + '.csv')


#lambda_handler(event={"tables":["sales_order", "design"]},context={})
