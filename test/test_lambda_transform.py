from src.lambda_transform import get_latest_ingested_tables,get_latest_transformed_tables,get_new_tables,transform_where_new_tables, lambda_handler

# to_check = ["sales_order", "staff", "currency", "design", "counterparty", "date", "address", "department"]
# get_latest_ingested_tables('mourne-s3-totes-sys-ingestion-bucket', to_check)

# get_latest_transformed_tables('mock-transformed')

# get_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed')
# """^returns new_tables"""

# transform_where_new_tables('mourne-s3-totes-sys-ingestion-bucket','mock-transformed')

event = {
    "ingestion_bucket": "mourne-s3-totes-sys-ingestion-bucket",
    "transformed_bucket": "mock-transformed",
    "tables_to_check": ["sales_order", "staff", "currency", "design", "counterparty", "date", "address", "department"]
}
lambda_handler(event=event, context={})