import boto3
import os
from typing import Dict, Any
from helpers import fetch_credentials, export_db_creds_to_env
from load_utils.write_dataframe_to_dw import process_dim_tables, process_fact_tables
from load_utils.database_utils import create_connection
import logging

secret_client =  boto3.client("secretsmanager")
s3_client = boto3.client("s3")
#os.environ["BUCKET_NAME"] = "mourne-s3-totes-sys-transform-bucket-3"
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for loading parquet data from S3 to a data warehouse.

    This function processes an event containing lists of fact and dimension tables
    and their S3 keys. It reads parquet files from S3, and writes the data
    into a data warehouse. Fact tables are inserted, and dimension tables are replaced.

    Args:
        event (Dict[str, Any]): Event data containing table information and S3 keys.
                                 Expected format:
                                 {
                                     "status_code": int,
                                     "fact_tables": {"table_name": "s3_key", ...},
                                     "dim_tables": {"table_name": "s3_key", ...}
                                 }
        context (Any): Lambda context object (not used in this function).

    Returns:
        Dict[str, Any]: Status code and message indicating success or failure.
                         Example: {"status_code": 200, "body": "Data load completed"}

    Raises:
        TypeError: If event is not a dictionary.
        ValueError: If event is missing required keys ('fact_tables' or 'dim_tables').
        Exception: If any unexpected error occurs during processing.
    """
    db_conn = None

    if not isinstance(event, dict):
        raise TypeError("event must be a dictionary")
    if "fact_tables" not in event:
        raise ValueError("event must contain 'fact_tables'")
    if "dim_tables" not in event:
        raise ValueError("event must contain 'dim_tables'")

    fact_tables = event.get("fact_tables", {})
    dim_tables = event.get("dim_tables", {})

    s3_client = boto3.client('s3')

    try:
        db_conn = get_connection()
        process_dim_tables(dim_tables, s3_client, db_conn)
        logging.info(f"Datawarehouse update for dim tables complete: {dim_tables}")
        process_fact_tables(fact_tables, s3_client, db_conn)
        logging.info(f"Datawarehouse update for fact tables complete: {fact_tables}")
        return {"status_code": 200, "body": "Data load completed"}
    except Exception as e:
        logging.info(f"loading failed: {e}")        
        return {"status_code": 500, "body": f"Error processing data load: {str(e)}"}
    finally:
        if db_conn:
            db_conn.close()

def get_connection(secret_client = secret_client):
    db_creds = fetch_credentials(secret_client, secret_name="warehouse_credentials")
    export_db_creds_to_env(db_creds, ["username","password","port","host"])
    conn = create_connection()
    return conn


# lambda_handler({  "fact_tables": {
#     "fact_sales_order": "2025/03/11/14/58/fact_sales_order.parquet",
#     #"fact_payment": "2025/03/11/14/58/fact_payment.parquet",
#     #"fact_purchase_order": "2025/03/11/14/58/fact_purchase_order.parquet"
#   },
#   "dim_tables": {
#     "dim_date": "2025/03/11/14/58/dim_date.parquet",
#     "dim_staff": "2025/03/11/14/58/dim_staff.parquet",
#     "dim_counterparty": "2025/03/11/14/58/dim_counterparty.parquet",
#     "dim_currency": "2025/03/11/14/58/dim_currency.parquet",
#     "dim_design": "2025/03/11/14/58/dim_design.parquet",
#     "dim_location": "2025/03/11/14/58/dim_location.parquet",
#     "dim_payment_type": "2025/03/11/14/58/dim_payment_type.parquet",
#     "dim_transaction": "2025/03/11/14/58/dim_transaction.parquet"
#   }}, {})

#refactor the utilities
#refractor tests
#make sure it works as lambda function


#add sqlalchemy to the layer
