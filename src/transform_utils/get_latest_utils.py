import boto3
from datetime import datetime, UTC

s3_client = boto3.client('s3')

def get_latest_ingested_tables(ingestion_bucket, tables_to_check):
    """
    get the latest verison of each table from ingestion
    """
    ing_response = s3_client.list_objects_v2(Bucket=ingestion_bucket).get("Contents", [])
    latest_ingested_tables = {}

    for file in ing_response:
        table_name = file['Key'].split('/')[-1][0:-4]
        if table_name in tables_to_check:
            if table_name not in latest_ingested_tables or file['LastModified'] > latest_ingested_tables.get(table_name, [None, datetime(1900, 11, 21, 16, 30, tzinfo=UTC)])[1]:
                latest_ingested_tables[table_name] = [file['Key'],file['LastModified']]
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
