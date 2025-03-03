import pytest
import boto3
import pandas as pd
from moto import mock_aws
from src.load_utils.read_parquet import read_parquet_from_s3
from botocore.exceptions import ClientError
from pandas.errors import ParserError
from io import BytesIO
import os
from unittest.mock import patch


@pytest.fixture
def aws_setup():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        os.environ["BUCKET_NAME"] = bucket_name
        s3_client.create_bucket(Bucket=bucket_name)
        yield s3_client, bucket_name
    del os.environ["BUCKET_NAME"]


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})


@pytest.fixture
def empty_dataframe():
    return pd.DataFrame()


@pytest.fixture
def parquet_file_in_s3(aws_setup, sample_dataframe, empty_dataframe):
    s3_client, bucket_name = aws_setup
    sample_s3_key = "test_file.parquet"
    empty_s3_key = "empty_file.parquet"
    subdir_s3_key = "sub_dir/test_file.parquet"

    buffer_sample = BytesIO()
    sample_dataframe.to_parquet(buffer_sample)
    buffer_sample.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=sample_s3_key, Body=buffer_sample)
    s3_client.put_object(Bucket=bucket_name, Key=subdir_s3_key, Body=buffer_sample)

    buffer_empty = BytesIO()
    empty_dataframe.to_parquet(buffer_empty)
    buffer_empty.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=empty_s3_key, Body=buffer_empty)

    return (
        s3_client,
        bucket_name,
        sample_s3_key,
        empty_s3_key,
        subdir_s3_key,
        sample_dataframe,
        empty_dataframe,
    )


class TestReadParquetFromS3:
    def test_giving_valid_s3_key_and_client_when_read_parquet_from_s3_then_return_dataframe(
        self, parquet_file_in_s3, sample_dataframe
    ):
        s3_client, _, s3_key, _, _, expected_df, _ = parquet_file_in_s3

        result_df = read_parquet_from_s3(s3_client, s3_key)

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_giving_parquet_file_with_data_when_read_parquet_from_s3_then_dataframe_contains_data(
        self, parquet_file_in_s3, sample_dataframe
    ):
        s3_client, _, s3_key, _, _, expected_df, _ = parquet_file_in_s3

        result_df = read_parquet_from_s3(s3_client, s3_key)

        assert not result_df.empty
        assert list(result_df.columns) == ["col1", "col2"]
        assert result_df.iloc[0, 0] == 1
        assert result_df.iloc[1, 1] == 4

    def test_giving_parquet_file_empty_when_read_parquet_from_s3_then_return_empty_dataframe(
        self, parquet_file_in_s3, empty_dataframe
    ):
        s3_client, _, _, empty_s3_key, _, _, _ = parquet_file_in_s3

        result_df = read_parquet_from_s3(s3_client, empty_s3_key)

        assert result_df.empty

    def test_giving_invalid_s3_key_type_when_read_parquet_from_s3_then_raise_type_error(
        self, aws_setup
    ):
        s3_client, _ = aws_setup

        with pytest.raises(TypeError):
            read_parquet_from_s3(s3_client, 123)

    def test_giving_empty_s3_key_when_read_parquet_from_s3_then_raise_value_error(
        self, aws_setup
    ):
        s3_client, _ = aws_setup

        with pytest.raises(ValueError) as excinfo:
            read_parquet_from_s3(s3_client, "")
        assert "S3 key cannot be empty" in str(excinfo.value)

    def test_giving_s3_key_with_leading_slash_when_read_parquet_from_s3_then_raise_value_error(
        self, aws_setup
    ):
        s3_client, _ = aws_setup

        with pytest.raises(ValueError) as excinfo:
            read_parquet_from_s3(s3_client, "/file.parquet")
        assert "Invalid S3 key format" in str(excinfo.value)

    def test_giving_non_existent_s3_key_when_read_parquet_from_s3_then_raise_client_error_file_not_found(
        self, aws_setup
    ):
        s3_client, _ = aws_setup
        s3_key = "non_existent_file.parquet"

        with pytest.raises(ClientError) as excinfo:
            read_parquet_from_s3(s3_client, s3_key)
        assert "NoSuchKey" in str(excinfo.value)

    def test_giving_s3_access_denied_when_read_parquet_from_s3_then_raise_client_error_access_denied(
        self, aws_setup
    ):
        s3_client, _ = aws_setup
        s3_key = "test_file.parquet"

        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        with patch.object(
            s3_client,
            "get_object",
            side_effect=ClientError(error_response, "GetObject"),
        ):
            with pytest.raises(ClientError) as excinfo:
                read_parquet_from_s3(s3_client, s3_key)
            assert "AccessDenied" in str(excinfo.value)

    def test_giving_corrupted_parquet_file_when_read_parquet_from_s3_then_raise_parser_error(
        self, aws_setup
    ):
        s3_client, bucket_name = aws_setup
        s3_key = "test_file.parquet"

        s3_client.put_object(
            Bucket=bucket_name, Key=s3_key, Body=b"This is not a parquet file"
        )

        with pytest.raises(ParserError):
            read_parquet_from_s3(s3_client, s3_key)

    def test_giving_s3_client_and_s3_key_when_read_parquet_from_s3_then_not_modify_input_arguments(
        self, parquet_file_in_s3
    ):
        s3_client, _, s3_key, _, _, _, _ = parquet_file_in_s3

        initial_s3_client = s3_client
        initial_s3_key = s3_key

        read_parquet_from_s3(s3_client, s3_key)

        assert initial_s3_client == s3_client
        assert initial_s3_key == s3_key

    def test_giving_no_s3_bucket_name_env_var_when_read_parquet_from_s3_then_raise_value_error(
        self, aws_setup
    ):
        s3_client, _ = aws_setup
        s3_key = "test_file.parquet"
        del os.environ["BUCKET_NAME"]

        with pytest.raises(ValueError) as excinfo:
            read_parquet_from_s3(s3_client, s3_key)
        assert "BUCKET_NAME environment variable not set" in str(excinfo.value)
        os.environ["BUCKET_NAME"] = "test-bucket"
