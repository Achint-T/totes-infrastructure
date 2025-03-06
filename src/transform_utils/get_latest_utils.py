import boto3
from datetime import datetime
import logging
from datetime import datetime

s3_client = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def extract_timestamp_from_filename(filename: str) -> datetime:
    parts = filename.split('/')
    y,m,d,h,mi = map(int,parts[:5])
    return datetime(y,m,d,h,mi)

def get_latest_ingested_tables(ingestion_bucket, tables_to_check):
    """
    get the latest version of each table from ingestion.

    arguments: takes name of ingestion_bucket, list of tables_to_check (e.g ["sales_order", "staff", "design"])

    Returns a dictionary with keys of table names and values of a list of 
    csv filename, LastModified metadata. 
    e.g: {'address': ['2025/3/5/17/0/address.csv', datetime.datetime(2025, 3, 5, 17, 0)]}

    """
    ing_response = s3_client.list_objects_v2(Bucket=ingestion_bucket).get("Contents", [])

    if len(ing_response) == 0:
        logging.info('Ingestion bucket is empty. Nothinig to transform')
    
    for t in tables_to_check:
        if t not in ing_response:
            logging.info(f'table_to_check {t} not present in ingestion bucket')

    latest_ingested_tables = {}

    for file in ing_response:
        table_name = file['Key'].split('/')[-1][0:-4]
        file_timestamp = extract_timestamp_from_filename(file['Key'])
        if table_name in tables_to_check:
            if table_name not in latest_ingested_tables or file_timestamp > latest_ingested_tables.get(table_name, [None, datetime(1500, 3, 5, 17, 0)])[1]:
                latest_ingested_tables[table_name] = [file['Key'],file_timestamp]

    print(f'LATEST INGESTED TABLES --------> {latest_ingested_tables}')
    return latest_ingested_tables

def get_latest_transformed_tables(transformed_bucket):
    """gets latest version of each star schema table from transformed bucket"""
    trans_response = s3_client.list_objects_v2(Bucket=transformed_bucket).get("Contents",[])
    latest_trans_tables = {}
    for file in trans_response:
        table_name = file['Key'].split('/')[0]
        if table_name not in latest_trans_tables or file['LastModified'] > latest_trans_tables[table_name][1]:
            latest_trans_tables[table_name] = [file['Key'],file['LastModified']]
    
    print(f'latest tranformed tables -------->{latest_trans_tables}')
    return latest_trans_tables

def get_new_tables(ingestion_bucket, transformed_bucket):
    """for each table in transformed bucket (e.g fact_sales_order, dim_staff etc) check if there is a newer
    version of any of the dependency tables in ingestion. the dependency tables of dim_staff are staff
    and department, for example.
    
    check by comparing LastModified field of metadata of ingestion bucket dependecy files
    using s3_client.list_objects_v2(Bucket=ingestion_bucket)
    with max LastModified field of resultant star schema tables in transformed bucket"""

    table_relations = {'fact_sales_order': ['sales_order'],
                       'dim_staff': ['staff','department'],
                       'dim_counterparty': ['counterparty', 'address'],
                    #    'dim_currency': ['sales_order','currency'],
                    #    'dim_date': ['date'],
                       'dim_design': ['design'],
                       'dim_location': ['address']
    }

    # determine new tables (where there's a newer version of a dependency table in ingested than
    # has been used to construct resultant star schema table in transformed bucket):

    new_tables = {}
    tables_to_check = {dep for deps in table_relations.values() for dep in deps}
    latest_ingested_tables = get_latest_ingested_tables(ingestion_bucket,tables_to_check)
    latest_transformed_tables = get_latest_transformed_tables(transformed_bucket)

    for star_table, dependencies in table_relations.items():
            for dep in dependencies:
                if latest_ingested_tables[dep][1] > latest_transformed_tables.get(star_table, [None, datetime(1900, 11, 21, 16, 30, tzinfo=UTC)])[1]:
                    new_tables[dep] = latest_ingested_tables[dep]

    print(f'NEW TABLES ------->{new_tables}')
    return new_tables
