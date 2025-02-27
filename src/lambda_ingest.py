import boto3
from helpers import fetch_credentials, export_db_creds_to_env
from ingestion_utils.file_utils import data_to_csv
from ingestion_utils.database_utils import create_connection, close_db_connection, get_recent_additions, get_last_upload_date
import time
import logging

def lambda_handler(event,context):
    #csv key for s3
    #update the lastupdatedate in secrets manager
    try:
        
        secret_client =  boto3.client("secretsmanager")
        db_creds = fetch_credentials(secret_client, secret_name="database_credentials")
        export_db_creds_to_env(db_creds, ["username","password","port","host"])

        conn = create_connection()

        last_date = get_last_upload_date(secret_client)

        
        tables_to_ingest = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction']
        
        timenow = list(time.gmtime()[:5]) + [time.gmtime()[6]-1]
        date = '-'.join([str(number).rjust(2,'0') for number in timenow[:3]])
        hours = ':'.join([str(number).rjust(2,'0') for number in timenow[3:]])
        timestamp = f'{date} {hours}'

        for table in tables_to_ingest:
            data = get_recent_additions(conn, tablename=table, updatedate=last_date, time_now=timestamp)
            data_to_csv(data, table_name=table)
            s3_client = boto3.client("s3")
            s3_client.upload_file(f"./{table}.csv", "s3-totes-sys-ingestion-bucket-20250227154311511600000002", '/'.join(map(str,timenow)) + '/' + table + '.csv')


        close_db_connection(conn)
        return {"statusCode": 200, "body": "placeholder lambda executed successfully"}
    except Exception as e:
        #close_db_connection(conn)
        logging.info(msg=f"failed {e}")
        return ""
    
lambda_handler({},{})