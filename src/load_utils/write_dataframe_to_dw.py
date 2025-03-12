import pandas as pd
import pg8000
import pg8000.native
from pg8000.native import literal
from typing import Dict
import boto3
from datetime import datetime
import logging
from botocore.exceptions import ClientError
from pandas.errors import ParserError
import os
import pyarrow.lib
from io import BytesIO

def write_dataframe_to_db(
    dataframe: pd.DataFrame, conn: pg8000.native.Connection, table_name: str, insert_mode: bool = True
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
    if not isinstance(table_name, str):
        raise TypeError("table_name must be a string")

    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")
    if not table_name:
        raise ValueError("table_name cannot be empty")

    try:
        sql = construct_sql(dataframe=dataframe, table_name=table_name, upsert=not insert_mode)
        conn.run(sql=sql)
        return True
    except pg8000.Error as db_error:
        raise pg8000.Error(str(db_error)) from db_error
    except Exception as e:
        raise Exception(str(e))
    

def process_tables(tables: Dict[str, str], s3_client: boto3.client, db_conn: pg8000.native.Connection, is_fact: bool = True) -> None:
    """Processes tables by loading data from S3 and inserting into the data warehouse.

    For each fact table, this function reads the corresponding parquet file from S3
    using the provided S3 key and inserts the data into the specified table in the
    data warehouse.

    Args:
        tables (Dict[str, str]): Dictionary of fact table names and their S3 keys.
                                       Example: {"sales_order": "path/sales_order.parquet", ...}
        s3_client (boto3.client): Boto3 S3 client.
        db_conn (pg8000.Connection): Database connection.

    Raises:
        TypeError: If fact_tables is not a dictionary, s3_client is not boto3 client, or db_conn is not pg8000 Connection.
        ValueError: If fact_tables is empty.
        Exception: If any error occurs during S3 read or database write operations.
    """
    if not isinstance(tables, dict):
        raise TypeError("fact_tables must be a dictionary")
    # if not isinstance(db_conn, pg8000.native.Connection):
    #     raise TypeError("db_conn must be a pg8000 Connection")

    if not tables:
        if not is_fact:
            raise ValueError("dim_tables cannot be empty")
        else:
            logging.info("fact_tables is empty, there is no update")
    else:
        for table_name, s3_key in tables.items():
            try:
                df = read_parquet_from_s3(s3_client, s3_key)
                write_dataframe_to_db(df, db_conn, table_name, insert_mode=is_fact)
            except Exception as e:
                raise Exception(f"Error processing table '{table_name}': {str(e)}") from e

def construct_sql(dataframe: pd.DataFrame, table_name: str, upsert: bool = True) -> str:
    """Constructs an SQL INSERT or UPSERT statement from a Pandas DataFrame.

    This function generates an SQL query string to insert data from a DataFrame
    into a specified table. It supports both regular INSERT and UPSERT (ON CONFLICT)
    statements based on the 'upsert' parameter.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing the data to be inserted.
            Columns of the DataFrame should correspond to the table columns.
        table_name (str): The name of the SQL table to insert data into.
        upsert (bool, optional):  If True, generates an UPSERT statement
            (ON CONFLICT DO UPDATE). If False, generates a regular INSERT statement.
            Defaults to True.

    Returns:
        str: The generated SQL query string.
    """
    columns = dataframe.columns
    formatted_columns = ', '.join([f'"{col}"' if ' ' in col or '-' in col or '.' in col else col for col in columns])
    values_clause = []
    for _, row in dataframe.iterrows():
        value_list = []
        for value in row:
            if pd.isna(value):
                value_list.append('NULL')
            elif isinstance(value, str):
                value_list.append(literal(value))
            elif isinstance(value, bool):
                value_list.append(str(value).upper())
            elif isinstance(value, datetime):
                value_list.append(literal(value.strftime("%Y-%m-%d")))
            else:
                value_list.append(literal(value))
        values_clause.append(f"({', '.join(value_list)})")

    if not values_clause:
        values_str = "()"
    else:
        values_str = ', '.join(values_clause)

    if upsert:
        set_clause = ', '.join([f"{col}=excluded.{col}" for col in columns[1:]])
        sql_statement = f"INSERT INTO {table_name} ({formatted_columns}) VALUES {values_str}\nON CONFLICT ({columns[0]}) DO UPDATE SET {set_clause};"
    else:
        sql_statement = f"INSERT INTO {table_name} ({formatted_columns}) VALUES {values_str};"
    return sql_statement

def read_parquet_from_s3(s3_client: boto3.client, s3_key: str) -> pd.DataFrame:
    """Reads Parquet file from S3 to Pandas DataFrame.
    Bucket name is expected to be in environment variable 'BUCKET_NAME'.

    Args:
        s3_client (boto3.client): Boto3 S3 client.
        s3_key (str): S3 key of the Parquet file (e.g., 'path/file.parquet').
                      Do not include the bucket name here.

    Returns:
        pd.DataFrame: DataFrame from Parquet data.

    Raises:
        TypeError: If `s3_client` is not boto3 client or `s3_key` not str.
        ValueError: If `s3_key` is empty or invalid, or if 'S3_BUCKET_NAME' env var is not set.
        ClientError: For S3 related issues (file not found, access denied).
        ParserError: If Parquet parsing fails (corrupted file).

    Example:
        >>> import os
        >>> os.environ['S3_BUCKET_NAME'] = 'your-bucket-name'
        >>> s3 = boto3.client('s3')
        >>> df = read_parquet_from_s3(s3, 'path/data.parquet')
        >>> if df is not None:
        ...     print(df.head())
    """

    if not isinstance(s3_key, str):
        raise TypeError("s3_key must be a string")
    if not s3_key:
        raise ValueError("S3 key cannot be empty")
    if s3_key.startswith("/"):
        raise ValueError(
            "Invalid S3 key format. S3 key should not start with a leading slash."
        )

    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable not set")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        parquet_file = response["Body"]
        df = pd.read_parquet(BytesIO(parquet_file.read()))
        return df
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise ClientError(
                error_response={
                    "Error": {"Code": "NoSuchKey", "Message": "File not found in S3"}
                },
                operation_name="GetObject",
            ) from e
        elif e.response["Error"]["Code"] == "AccessDenied":
            raise ClientError(
                error_response={
                    "Error": {
                        "Code": "AccessDenied",
                        "Message": "Access denied to S3 object",
                    }
                },
                operation_name="GetObject",
            ) from e
        raise
    except (ParserError, pyarrow.lib.ArrowInvalid) as e:
        raise ParserError(
            "Failed to parse Parquet file. File might be corrupted."
        ) from e