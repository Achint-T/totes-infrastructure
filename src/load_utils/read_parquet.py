import boto3
import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError
from pandas.errors import ParserError
import os
import pyarrow.lib


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
