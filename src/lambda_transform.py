 # get csv files from ingestion bucket - async
# logic to take only new/updated files

# returns pd dataframe. Each dataframe is assigned a variable name df_tablename. 

# run fact_sales_orders util on dataframe - outputs new dataframe
# run all dimension utils (taking from fact dataframe)
# write to parquet util function on all resulting dataframes - async

# general client error handling

from src.transform_utils.file_utils import read_csv_from_s3, write_parquet_to_s3
from src.transform_utils.fact_sales_order import util_fact_sales_order
from src.transform_utils.dim_staff import util_dim_staff
from src.transform_utils.dim_counterparty import util_dim_counterparty
# from src.transform_utils.dim_currency import util_dim_currency
from src.transform_utils.dim_date import util_dim_date
from src.transform_utils.dim_design import util_dim_design
from src.transform_utils.dim_location import util_dim_location
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, UTC
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event,context):
    #no need to pass bucket names do env vars
    """Event example:
    {
        "ingestion_bucket": "fake-ingested",
        "transformed_bucket": "mock-transformed",
        "tables_to_check": ["sales_order", "staff", "currency", "design", "counterparty", "date", "address", "department"]
    } 
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
                       'dim_currency': ['sales_order','currency'],
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

def transform_where_new_tables(ingestion_bucket, transformed_bucket):
    """runs relevant transformation util (returning pandas dataframe) where a star schema table has new dependency tables"""

    table_relations = {'fact_sales_order': ['sales_order'],
                    'dim_staff': ['staff','department'],
                    'dim_counterparty': ['counterparty', 'address'],
                    'dim_currency': ['sales_order','currency'],
                    'dim_date': ['date'],
                    'dim_design': ['design'],
                    'dim_location': ['address']
    }
    #determine which star schema tables need to be updated (have new dependency table versions):

    latest_trans_tables = get_latest_transformed_tables(transformed_bucket)
    new_tables = get_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed')
    tables_to_check = {dep for deps in table_relations.values() for dep in deps}
    latest_ingested_tables = get_latest_ingested_tables(ingestion_bucket,tables_to_check)
    
    s_tables_to_update = []

    for s_table in table_relations:
        if any(x in table_relations[s_table] for x in new_tables.keys()) or s_table not in latest_trans_tables:
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



"""RUN GET_NEW_TABLES"""

# get_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed',['sales_order','staff','currency', 'design','counterparty','date',
#                     'address', 'department'])

"""RUN TRANSFORM_WHERE_NEW_TABLES"""

# transform_where_new_tables(get_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed',['sales_order','staff','currency', 'design','counterparty','date',
#                     'address', 'department'])['new_tables'],get_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed',['sales_order','staff','currency', 'design','counterparty','date',
#                     'address', 'department'])['latest_ing_tables'],'mourne-s3-totes-sys-ingestion-bucket','mock-transformed')
    
"""RUN LAMBDA_HANDLER"""

event = {
    "ingestion_bucket": "mourne-s3-totes-sys-ingestion-bucket",
    "transformed_bucket": "mock-transformed",
    "tables_to_check": ["sales_order", "staff", "currency", "design", "counterparty", "date", "address", "department"]
}
# lambda_handler(event=event, context={})

"""RUN get_latest_ingested_tables"""

# to_check = ["sales_order", "staff", "currency", "design", "counterparty", "date", "address", "department"]
# get_latest_ingested_tables('mourne-s3-totes-sys-ingestion-bucket', to_check)
