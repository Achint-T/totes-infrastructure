import pytest
from unittest import mock
from src.load_utils.write_dataframe_to_dw import process_tables  # Assuming process_tables is in src/task_name.py
import logging

class TestProcessTables:

    @pytest.fixture
    def mock_s3_client(self):
        with mock.patch("src.load_utils.write_dataframe_to_dw.boto3.client") as mock_client:
            yield mock_client.return_value

    @pytest.fixture
    def mock_db_conn(self):
        with mock.patch("src.load_utils.write_dataframe_to_dw.pg8000.native.Connection", autospec=True) as MockConnection:
            yield MockConnection.return_value

    @pytest.fixture
    def mock_read_parquet_from_s3(self):
        with mock.patch("src.load_utils.write_dataframe_to_dw.read_parquet_from_s3") as mock_read:
            yield mock_read

    @pytest.fixture
    def mock_write_dataframe_to_db(self):
        with mock.patch("src.load_utils.write_dataframe_to_dw.write_dataframe_to_db") as mock_write:
            yield mock_write

    def test_giving_invalid_tables_type_when_process_tables_then_raise_type_error(self, mock_s3_client, mock_db_conn):
        with pytest.raises(TypeError) as excinfo:
            process_tables(tables="not_a_dict", s3_client=mock_s3_client, db_conn=mock_db_conn)
        assert str(excinfo.value) == "fact_tables must be a dictionary"

    def test_giving_empty_dim_tables_when_is_fact_false_then_raise_value_error(self, mock_s3_client, mock_db_conn):
        with pytest.raises(ValueError) as excinfo:
            process_tables(tables={}, s3_client=mock_s3_client, db_conn=mock_db_conn, is_fact=False)
        assert str(excinfo.value) == "dim_tables cannot be empty"

    def test_giving_empty_fact_tables_when_is_fact_true_then_no_error_and_log_info(self, mock_s3_client, mock_db_conn, caplog):
        caplog.set_level(logging.INFO)
        process_tables(tables={}, s3_client=mock_s3_client, db_conn=mock_db_conn, is_fact=True)
        assert "fact_tables is empty, there is no update" in caplog.text

    def test_giving_valid_tables_when_process_tables_then_read_from_s3_and_write_to_db(self, mock_s3_client, mock_db_conn, mock_read_parquet_from_s3, mock_write_dataframe_to_db):
        tables = {"table1": "s3://bucket/path/table1.parquet", "table2": "s3://bucket/path/table2.parquet"}
        mock_read_parquet_from_s3.return_value = "mock_dataframe"

        process_tables(tables=tables, s3_client=mock_s3_client, db_conn=mock_db_conn)

        mock_read_parquet_from_s3.assert_has_calls([
            mock.call(mock_s3_client, "s3://bucket/path/table1.parquet"),
            mock.call(mock_s3_client, "s3://bucket/path/table2.parquet")
        ])
        mock_write_dataframe_to_db.assert_has_calls([
            mock.call("mock_dataframe", mock_db_conn, "table1", insert_mode=True),
            mock.call("mock_dataframe", mock_db_conn, "table2", insert_mode=True)
        ])

    def test_giving_valid_dim_tables_when_is_fact_false_then_read_from_s3_and_write_to_db_with_insert_mode_false(self, mock_s3_client, mock_db_conn, mock_read_parquet_from_s3, mock_write_dataframe_to_db):
        tables = {"dim_table1": "s3://bucket/path/dim_table1.parquet"}
        mock_read_parquet_from_s3.return_value = "mock_dataframe"

        process_tables(tables=tables, s3_client=mock_s3_client, db_conn=mock_db_conn, is_fact=False)

        mock_read_parquet_from_s3.assert_called_once_with(mock_s3_client, "s3://bucket/path/dim_table1.parquet")
        mock_write_dataframe_to_db.assert_called_once_with("mock_dataframe", mock_db_conn, "dim_table1", insert_mode=False)

    def test_giving_s3_read_error_when_process_tables_then_raise_exception(self, mock_s3_client, mock_db_conn, mock_read_parquet_from_s3):
        tables = {"table1": "s3://bucket/path/table1.parquet"}
        mock_read_parquet_from_s3.side_effect = Exception("S3 read error")

        with pytest.raises(Exception) as excinfo:
            process_tables(tables=tables, s3_client=mock_s3_client, db_conn=mock_db_conn)

        assert str(excinfo.value) == "Error processing table 'table1': S3 read error"

    def test_giving_db_write_error_when_process_tables_then_raise_exception(self, mock_s3_client, mock_db_conn, mock_read_parquet_from_s3, mock_write_dataframe_to_db):
        tables = {"table1": "s3://bucket/path/table1.parquet"}
        mock_write_dataframe_to_db.side_effect = Exception("DB write error")
        mock_read_parquet_from_s3.return_value = "mock_dataframe"

        with pytest.raises(Exception) as excinfo:
            process_tables(tables=tables, s3_client=mock_s3_client, db_conn=mock_db_conn)

        assert str(excinfo.value) == "Error processing table 'table1': DB write error"

    def test_giving_valid_tables_when_process_tables_then_input_tables_not_modified(self, mock_s3_client, mock_db_conn, mock_read_parquet_from_s3, mock_write_dataframe_to_db):
        tables_input = {"table1": "s3://bucket/path/table1.parquet"}
        tables_original = tables_input.copy()

        process_tables(tables=tables_input, s3_client=mock_s3_client, db_conn=mock_db_conn)

        assert tables_input == tables_original