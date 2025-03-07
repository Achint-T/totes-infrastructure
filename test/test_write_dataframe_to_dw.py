import pytest
import pandas as pd
import pg8000
from unittest.mock import MagicMock
import boto3
from src.load_utils.write_dataframe_to_dw import write_dataframe_to_db, process_dim_tables, process_fact_tables
from unittest.mock import patch
import os 

class TestWriteDataframeToDb:
    @pytest.fixture
    def mock_dataframe(self):
        return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    @pytest.fixture
    def empty_dataframe(self):
        return pd.DataFrame()

    @pytest.fixture
    def mock_conn(self):
        return MagicMock(spec=pg8000.Connection)

    def test_giving_invalid_dataframe_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_conn
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db("not_a_dataframe", mock_conn, "test_table")

    def test_giving_invalid_conn_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_dataframe
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db(mock_dataframe, "not_a_conn", "test_table")

    def test_giving_invalid_table_name_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_dataframe, mock_conn
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db(mock_dataframe, mock_conn, 123)

    def test_giving_invalid_insert_mode_type_when_write_dataframe_to_db_then_raise_typeerror(
        self, mock_dataframe, mock_conn
    ):
        with pytest.raises(TypeError):
            write_dataframe_to_db(mock_dataframe, mock_conn, "test_table", insert_mode="not_a_bool")

    def test_giving_empty_dataframe_when_write_dataframe_to_db_then_raise_valueerror(
        self, empty_dataframe, mock_conn
    ):
        with pytest.raises(ValueError):
            write_dataframe_to_db(empty_dataframe, mock_conn, "test_table")

    def test_giving_empty_table_name_when_write_dataframe_to_db_then_raise_valueerror(
        self, mock_dataframe, mock_conn
    ):
        with pytest.raises(ValueError):
            write_dataframe_to_db(mock_dataframe, mock_conn, "")

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_insert_mode_then_return_true(
        self, mock_dataframe, mock_conn
    ):
        mock_conn.run.return_value = None
        result = write_dataframe_to_db(mock_dataframe, mock_conn, "test_table", insert_mode=True)
        assert result is True

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_replace_mode_then_return_true(
        self, mock_dataframe, mock_conn
    ):
        mock_conn.run.return_value = None
        result = write_dataframe_to_db(mock_dataframe, mock_conn, "test_table", insert_mode=False)
        assert result is True

    def test_giving_db_error_when_write_dataframe_to_db_then_raise_pg8000_error(
        self, mock_dataframe, mock_conn
    ):
        mock_conn.run.side_effect = pg8000.Error("Database error")
        with pytest.raises(pg8000.Error) as excinfo:
            write_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        assert "Database error" in str(excinfo.value)

    def test_giving_valid_dataframe_when_write_dataframe_to_db_then_not_modify_dataframe(
        self, mock_dataframe, mock_conn
    ):
        dataframe_copy = mock_dataframe.copy()
        write_dataframe_to_db(mock_dataframe, mock_conn, "test_table")
        pd.testing.assert_frame_equal(mock_dataframe, dataframe_copy)

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_insert_mode_then_execute_correct_insert_sql(
        self, mock_dataframe, mock_conn
    ):
        table_name = "test_table"
        write_dataframe_to_db(mock_dataframe, mock_conn, table_name, insert_mode=True)
        calls = mock_conn.run.call_args_list

        expected_sql_list = [
            f"INSERT INTO {table_name} (col1,col2) VALUES (%s,%s)",
            [[1, 'a'], [2, 'b']]
        ]
        assert len(calls) == 1
        assert calls[0].args[0] == expected_sql_list[0]
        assert calls[0].args[1] == expected_sql_list[1]

    def test_giving_valid_dataframe_and_conn_when_write_dataframe_to_db_in_replace_mode_then_execute_correct_replace_sql(
        self, mock_dataframe, mock_conn
    ):
        table_name = "test_table"
        write_dataframe_to_db(mock_dataframe, mock_conn, table_name, insert_mode=False)
        calls = mock_conn.run.call_args_list

        expected_sql_list = [
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} (col1 INTEGER,col2 TEXT)",
            f"INSERT INTO {table_name} (col1,col2) VALUES (%s,%s)",
            [[1, 'a'], [2, 'b']]
        ]
        assert len(calls) == 3
        assert calls[0].args[0] == expected_sql_list[0]
        assert calls[1].args[0] == expected_sql_list[1]
        assert calls[2].args[0] == expected_sql_list[2]
        assert calls[2].args[1] == expected_sql_list[3]

    def test_giving_dataframe_with_different_dtypes_when_write_dataframe_to_db_in_replace_mode_then_create_table_with_correct_dtypes(self, mock_conn):
        table_name = "dtype_test_table"
        dtype_df = pd.DataFrame({
            "int_col": [1, 2],
            "float_col": [1.1, 2.2],
            "str_col": ["a", "b"],
            "date_col": pd.to_datetime(['2023-10-26', '2023-10-27'])
        })
        write_dataframe_to_db(dtype_df, mock_conn, table_name, insert_mode=False)
        calls = mock_conn.run.call_args_list

        expected_create_table_sql = f"CREATE TABLE {table_name} (int_col INTEGER,float_col FLOAT,str_col TEXT,date_col TIMESTAMP)"

        assert calls[1].args[0] == expected_create_table_sql


class TestProcessFactTables:

    @pytest.fixture(autouse=True)
    def env_variable(self):
        variable_name = "BUCKET_NAME"
        variable_value = "DEMO_BUCKET"
        os.environ[variable_name] = variable_value

        yield

        if variable_name in os.environ:
            del os.environ[variable_name]

    @pytest.fixture
    def mock_fact_tables_config(self):
        return {"fact_table_1": "s3_key_fact_1", "fact_table_2": "s3_key_fact_2"}

    @pytest.fixture
    def mock_s3_client(self, env_variable):
        return MagicMock(spec=boto3.client)

    @pytest.fixture
    def mock_db_conn(self):
        return MagicMock(spec=pg8000.Connection)

    @pytest.fixture
    def mock_dataframe(self):
        return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    # #@patch("src.task_name.pd.read_parquet")
    # @patch("src.task_name.insert_dataframe_to_db")
    # def test_giving_valid_inputs_when_process_fact_tables_then_call_read_parquet_and_insert_dataframe_to_db_for_each_table(self, mock_insert_db, mock_read_parquet, mock_fact_tables_config, mock_s3_client, mock_db_conn, mock_dataframe):
    #     mock_read_parquet.return_value = mock_dataframe
    #     process_fact_tables(mock_fact_tables_config, mock_s3_client, mock_db_conn)
    #     assert mock_read_parquet.call_count == len(mock_fact_tables_config)
    #     assert mock_insert_db.call_count == len(mock_fact_tables_config)
    #     for table_name, s3_key in mock_fact_tables_config.items():
    #         mock_read_parquet.assert_any_call(f"s3://{os.environ['BUCKET_NAME']}/{s3_key}", storage_options={'client': mock_s3_client})
    #         mock_insert_db.assert_any_call(mock_dataframe, mock_db_conn, table_name)

    def test_giving_invalid_fact_tables_type_when_process_fact_tables_then_raise_typeerror(self, mock_s3_client, mock_db_conn):
        with pytest.raises(TypeError):
            process_fact_tables("not_a_dict", mock_s3_client, mock_db_conn)

    # def test_giving_invalid_s3_client_type_when_process_fact_tables_then_raise_typeerror(self, mock_fact_tables_config, mock_db_conn):
    #     with pytest.raises(TypeError):
    #         process_fact_tables(mock_fact_tables_config, "not_an_s3_client", mock_db_conn)

    def test_giving_invalid_db_conn_type_when_process_fact_tables_then_raise_typeerror(self, mock_fact_tables_config, mock_s3_client):
        with pytest.raises(TypeError):
            process_fact_tables(mock_fact_tables_config, mock_s3_client, "not_a_db_conn")

    # @patch("src.task_name.pd.read_parquet", side_effect=Exception("S3 read error"))
    # def test_giving_s3_read_error_when_process_fact_tables_then_raise_exception(self, mock_read_parquet, mock_fact_tables_config, mock_s3_client, mock_db_conn):
    #     with pytest.raises(Exception) as excinfo:
    #         process_fact_tables(mock_fact_tables_config, mock_s3_client, mock_db_conn)
    #     assert "S3 read error" in str(excinfo.value)

    # @patch("src.task_name.pd.read_parquet")
    # @patch("src.task_name.insert_dataframe_to_db", side_effect=Exception("DB insert error"))
    # def test_giving_db_insert_error_when_process_fact_tables_then_raise_exception(self, mock_insert_db, mock_read_parquet, mock_fact_tables_config, mock_s3_client, mock_db_conn, mock_dataframe):
    #     mock_read_parquet.return_value = mock_dataframe
    #     with pytest.raises(Exception) as excinfo:
    #         process_fact_tables(mock_fact_tables_config, mock_s3_client, mock_db_conn)
    #     assert "DB insert error" in str(excinfo.value)


class TestProcessDimTables:

    @pytest.fixture(autouse=True)
    def env_variable(self):
        variable_name = "BUCKET_NAME"
        variable_value = "DEMO_BUCKET"
        os.environ[variable_name] = variable_value

        yield

        if variable_name in os.environ:
            del os.environ[variable_name]
            
    @pytest.fixture
    def mock_dim_tables_config(self):
        return {"dim_table_1": "s3_key_dim_1", "dim_table_2": "s3_key_dim_2"}

    @pytest.fixture
    def mock_s3_client(self):
        return MagicMock(spec=boto3.client)

    @pytest.fixture
    def mock_db_conn(self):
        return MagicMock(spec=pg8000.Connection)

    @pytest.fixture
    def mock_dataframe(self):
        return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    # @patch("src.load_utils.read_parquet.pd.read_parquet")
    # @patch("src.load_utils.write_dataframe_to_dw.write_dataframe_to_db")
    # def test_giving_valid_inputs_when_process_dim_tables_then_call_read_parquet_and_replace_dataframe_to_db_for_each_table(self, mock_replace_db, mock_read_parquet, mock_dim_tables_config, mock_s3_client, mock_db_conn, mock_dataframe):
    #     mock_read_parquet.return_value = mock_dataframe
    #     process_dim_tables(mock_dim_tables_config, mock_s3_client, mock_db_conn)
    #     assert mock_read_parquet.call_count == len(mock_dim_tables_config)
    #     assert mock_replace_db.call_count == len(mock_dim_tables_config)
    #     for table_name, s3_key in mock_dim_tables_config.items():
    #         mock_read_parquet.assert_any_call(f"s3://{os.environ['BUCKET_NAME']}/{s3_key}", storage_options={'client': mock_s3_client})
    #         mock_replace_db.assert_any_call(mock_dataframe, mock_db_conn, table_name)

    def test_giving_invalid_dim_tables_type_when_process_dim_tables_then_raise_typeerror(self, mock_s3_client, mock_db_conn):
        with pytest.raises(TypeError):
            process_dim_tables("not_a_dict", mock_s3_client, mock_db_conn)

    # def test_giving_invalid_s3_client_type_when_process_dim_tables_then_raise_typeerror(self, mock_dim_tables_config, mock_db_conn):
    #     with pytest.raises(TypeError):
    #         process_dim_tables(mock_dim_tables_config, "not_an_s3_client", mock_db_conn)

    def test_giving_invalid_db_conn_type_when_process_dim_tables_then_raise_typeerror(self, mock_dim_tables_config, mock_s3_client):
        with pytest.raises(TypeError):
            process_dim_tables(mock_dim_tables_config, mock_s3_client, "not_a_db_conn")

#     @patch("src.task_name.pd.read_parquet", side_effect=Exception("S3 read error"))
#     def test_giving_s3_read_error_when_process_dim_tables_then_raise_exception(self, mock_read_parquet, mock_dim_tables_config, mock_s3_client, mock_db_conn):
#         with pytest.raises(Exception) as excinfo:
#             process_dim_tables(mock_dim_tables_config, mock_s3_client, mock_db_conn)
#         assert "S3 read error" in str(excinfo.value)

    # @patch("src.load_utils.read_parquet.pd.read_parquet")
    # @patch("src.load_utils.write_dataframe_to_dw.write_dataframe_to_db", side_effect=Exception("DB replace error"))
    # def test_giving_db_replace_error_when_process_dim_tables_then_raise_exception(self, mock_replace_db, mock_read_parquet, mock_dim_tables_config, mock_s3_client, mock_db_conn, mock_dataframe):
    #     mock_read_parquet.return_value = mock_dataframe
    #     with pytest.raises(Exception) as excinfo:
    #         process_dim_tables(mock_dim_tables_config, mock_s3_client, mock_db_conn)
    #     assert "DB replace error" in str(excinfo.value)