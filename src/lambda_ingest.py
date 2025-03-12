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
        fact_tables_to_ingest = event["fact_tables"]
        dim_tables_to_ingest = event["dim_tables"]

        conn = get_connection()
        last_date = get_last_run_date()
        time_now = time.gmtime()
        timestamp = get_current_time(time_now)

        fact_keys = save_data_to_s3(conn, fact_tables_to_ingest, timestamp, last_date )
        logging.info(f"Extraction run successfully for fact tables: {fact_tables_to_ingest}")
        dim_keys = save_data_to_s3(conn, dim_tables_to_ingest, timestamp)
        logging.info(f"Extraction run successfully for dimension tables: {dim_tables_to_ingest}")

        put_last_run_date(time_now)

        return {"status_code": 200, "fact_tables": fact_keys, "dim_tables" : dim_keys}

    except KeyError as ke:
        logging.error(f"Missing key in event: {ke}")
        return {"status_code": 400, "body": f"Missing table information: {ke}"}
    except Exception as e:
        logging.error(f"Extraction run failed: {e}")
        return {"status_code": 500, "body": f"Extraction run failed: {e}"} 
    finally:
        if conn:
            close_db_connection(conn)

def get_connection(secret_client = secret_client):
    """Creates a database connection using credentials from AWS secrets manager.

    Retrieves database credentials from the secrets manager. Exports these credentials
    as environment variables, and then creates a database connection.

    Args:
        secret_client (object): Client for accessing the secrets manager.

    Returns:
        object: Database connection instance.
    """
    db_creds = fetch_credentials(secret_client, secret_name="database_credentials")
    export_db_creds_to_env(db_creds, ["username","password","port","host"])
    conn = create_connection()
    return conn

def get_last_run_date(secret_client = secret_client):
    """Retrieves the timestamp of the last ingestion run.

    Fetches the last upload date stored in AWS Secrets Manager using the provided 
    secret client.

    Args:
        secret_client (object): Client for accessing the secrets manager.

    Returns:
        str: The last ingestion timestamp in the format 'YYYY-MM-DD 00:00:00'.
    """
    return get_last_upload_date(secret_client)

def put_last_run_date(timeobject, secret_client = secret_client):
    """Stores the timestamp of the latest ingestion run.

    Takes a time object (from `time.gmtime()` or a list in the format 
    [year, month, day, hour, minute, second]) and updates the secret named'lastupload'
    in AWS Secrets Manager.

    Args:
        time_object (object or list): Time object representing the last 
            run timestamp.
        secret_client (object): Client for accessing the secrets manager.

    Returns:
        None.

    Raises:
        Exception: If an error occurs while updating or creating the secret.
    """
    return put_last_upload_date(timeobject, secret_client)

def save_data_to_s3(conn, tables_to_ingest, timestamp, last_date = "2020-01-01 00:00:00", s3_client=s3_client) -> dict:
    """Saves latest additions to database to S3 bucket as CSV files.

    Retrieves data from specified tables that have been updated since the given 
    `last_date`, saves the data as CSV files, and uploads them to an S3 bucket.

    Args:
        conn (object): Database connection instance.
        tables_to_ingest (list): A list of specified tables to be uploaded.
        timestamp (dict): A dictionary containing timestamp information for the
            current lambda handler run.
        last_date (str): The timestamp for the last lambda handler run. Defaults
            to "2020-01-01 00:00:00".
        s3_client (object): Client for accessing the S3 bucket.

    Returns:
        dict: A dictionary with table names as keys and file paths as values.
    """
    key_dict = {}
    for table in tables_to_ingest:
        data = get_recent_additions(conn, tablename=table, updatedate=last_date, time_now=timestamp["secret"])
        key = timestamp["filepath"] + '/' + table + '.csv'
        if data["body"]:
            data_to_csv(data, table_name=table)
            s3_client.upload_file(f"/tmp/{table}.csv", bucket_name, key)
            key_dict[table]=key
    return key_dict