from pg8000.native import Connection
import boto3
import time
import re
import csv
import os
from dotenv import load_dotenv

def create_connection():
    connection = Connection(os.environ['USERNAME'], password=os.environ['PASSWORD'], host=os.environ['HOST'], port=os.environ['PORT'],database=os.environ['DBNAME'])
    return connection

def close_db_connection(conn):
    conn.close()

def get_last_upload_date(client):
    try:
        objects = client.list_objects_v2(Bucket='s3_ingestion_bucket')
        date_info = max([obj['Key'] for obj in objects['Contents'] if re.match(r"^20\d\d/\d+/\d+/\d+/\d+/",obj['Key'])]).split('/')
        return f'{date_info[0]}-{date_info[1].rjust(2,'0')}-{date_info[2].rjust(2,'0')} {date_info[3].rjust(2,'0')}:{date_info[4].rjust(2,'0')}:00'
    except:
        return None

def get_recent_additions(conn, tablename:str, updatedate):
    if updatedate == None:
        updatedate = '0000-00-00 00:00:00'
    data = conn.run(f'SELECT * FROM :tablename WHERE last_updated > :updatedate L;', tablename=tablename, updatedate=updatedate)
    headers = [col["name"] for col in conn.columns]
    return {'headers':headers, 'body':data}