import pandas as pd
import pg8000
from src.load_utils.read_parquet import read_parquet_from_s3
from typing import Dict
import boto3

def write_dataframe_to_db(
    dataframe: pd.DataFrame, conn: pg8000.Connection, table_name: str, insert_mode: bool = True
) -> bool:
    """Writes data from a Pandas DataFrame into a PostgreSQL table, with options for insert or replace.

    If insert_mode is True (default), data is inserted into an existing table.
    If insert_mode is False, the table is dropped if it exists and a new table is created
    based on the DataFrame schema before inserting data (replace mode).

    Args:
        dataframe (pd.DataFrame): DataFrame to write to the database.
        conn (pg8000.Connection): Database connection.
        table_name (str): Table name.
        insert_mode (bool, optional): True for insert mode, False for replace mode. Defaults to True.

    Returns:
        bool: True if write operation successful.

    Raises:
        TypeError: If input types are incorrect.
        ValueError: If DataFrame or table name is empty.
        pg8000.Error: If database error occurs.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a Pandas DataFrame")
    if not isinstance(conn, pg8000.Connection):
        raise TypeError("conn must be a pg8000 Connection")
    if not isinstance(table_name, str):
        raise TypeError("table_name must be a string")
    if not isinstance(insert_mode, bool):
        raise TypeError("insert_mode must be a boolean")

    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")
    if not table_name:
        raise ValueError("table_name cannot be empty")

    try:
        if not insert_mode:  # Replace mode when insert_mode is False
            conn.run(f"DROP TABLE IF EXISTS {table_name}")

            col_defs = []
            for col_name, dtype in dataframe.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    col_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    col_type = "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    col_type = "TIMESTAMP"
                else:
                    col_type = "TEXT"
                col_defs.append(f"{col_name} {col_type}")
            create_table_sql = f"CREATE TABLE {table_name} ({','.join(col_defs)})"
            conn.run(create_table_sql)

        cols_str = ",".join(dataframe.columns)
        placeholders_str = ",".join(["%s"] * len(dataframe.columns))
        sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders_str})"
        conn.run(sql, dataframe.values.tolist())

        return True
    except pg8000.Error as db_error:
        raise pg8000.Error(str(db_error)) from db_error
    

def process_fact_tables(fact_tables: Dict[str, str], s3_client: boto3.client, db_conn: pg8000.Connection) -> None:
    """Processes fact tables by loading data from S3 and inserting into the data warehouse.

    For each fact table, this function reads the corresponding parquet file from S3
    using the provided S3 key and inserts the data into the specified table in the
    data warehouse.

    Args:
        fact_tables (Dict[str, str]): Dictionary of fact table names and their S3 keys.
                                       Example: {"sales_order": "path/sales_order.parquet", ...}
        s3_client (boto3.client): Boto3 S3 client.
        db_conn (pg8000.Connection): Database connection.

    Raises:
        TypeError: If fact_tables is not a dictionary, s3_client is not boto3 client, or db_conn is not pg8000 Connection.
        ValueError: If fact_tables is empty.
        Exception: If any error occurs during S3 read or database write operations.
    """
    if not isinstance(fact_tables, dict):
        raise TypeError("fact_tables must be a dictionary")
    # if not isinstance(s3_client, boto3.client):
    #     print(type(s3_client))
    #     raise TypeError("s3_client must be a boto3 client")
    if not isinstance(db_conn, pg8000.Connection):
        raise TypeError("db_conn must be a pg8000 Connection")

    if not fact_tables:
        raise ValueError("fact_tables cannot be empty")

    for table_name, s3_key in fact_tables.items():
        try:
            df = read_parquet_from_s3(s3_client, s3_key)
            write_dataframe_to_db(df, db_conn, table_name, insert_mode=True)
        except Exception as e:
            raise Exception(f"Error processing fact table '{table_name}': {str(e)}") from e


def process_dim_tables(dim_tables: Dict[str, str], s3_client: boto3.client, db_conn: pg8000.Connection) -> None:
    """Processes dimension tables by loading data from S3 and replacing data in the data warehouse.

    For each dimension table, this function reads the corresponding parquet file from S3
    and replaces the entire content of the table in the data warehouse with the new data.
    This involves dropping and recreating the table if necessary before inserting data.

    Args:
        dim_tables (Dict[str, str]): Dictionary of dimension table names and their S3 keys.
                                       Example: {"design": "path/design.parquet", ...}
        s3_client (boto3.client): Boto3 S3 client.
        db_conn (pg8000.Connection): Database connection.

    Raises:
        TypeError: If dim_tables is not a dictionary, s3_client is not boto3 client, or db_conn is not pg8000 Connection.
        ValueError: If dim_tables is empty.
        Exception: If any error occurs during S3 read or database write operations.
    """
    if not isinstance(dim_tables, dict):
        raise TypeError("dim_tables must be a dictionary")
    # if not isinstance(s3_client, boto3.client):
    #     raise TypeError("s3_client must be a boto3 client")
    if not isinstance(db_conn, pg8000.Connection):
        raise TypeError("db_conn must be a pg8000 Connection")

    if not dim_tables:
        raise ValueError("dim_tables cannot be empty")

    for table_name, s3_key in dim_tables.items():
        try:
            df = read_parquet_from_s3(s3_client, s3_key)
            write_dataframe_to_db(df, db_conn, table_name, insert_mode=False)
        except Exception as e:
            raise Exception(f"Error processing dimension table '{table_name}': {str(e)}") from e
