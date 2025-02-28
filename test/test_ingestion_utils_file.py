import os
import pytest
import csv
from src.ingestion_utils.file_utils import data_to_csv

class TestDataToCsv:
    def test_giving_valid_data_and_tablename_when_data_to_csv_then_csv_file_created_with_correct_content(self):
        test_data = {
            'headers': ['col1', 'col2', 'col3'],
            'body': [
                ['row1_val1', 'row1_val2', 'row1_val3'],
                ['row2_val1', 'row2_val2', 'row2_val3']
            ]
        }
        table_name = 'test_table'
        csv_filename = f'{table_name}.csv'

        data_to_csv(test_data, table_name)

        assert os.path.exists(csv_filename)

        with open(csv_filename, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows == [['col1', 'col2', 'col3'],
                        ['row1_val1', 'row1_val2', 'row1_val3'],
                        ['row2_val1', 'row2_val2', 'row2_val3']]

        os.remove(csv_filename)

    def test_giving_data_missing_headers_when_data_to_csv_then_raises_valueerror(self):
        test_data = {
            'body': [['row1_val1']]
        }
        table_name = 'test_table'

        with pytest.raises(ValueError) as excinfo:
            data_to_csv(test_data, table_name)

        assert "Input 'data' dictionary must contain 'headers' and 'body' keys." in str(excinfo.value)

    def test_giving_data_missing_body_when_data_to_csv_then_raises_valueerror(self):
        test_data = {
            'headers': ['col1']
        }
        table_name = 'test_table'

        with pytest.raises(ValueError) as excinfo:
            data_to_csv(test_data, table_name)

        assert "Input 'data' dictionary must contain 'headers' and 'body' keys." in str(excinfo.value)

    def test_giving_non_dict_data_when_data_to_csv_then_raises_typeerror(self):
        test_data = "not a dictionary"
        table_name = 'test_table'

        with pytest.raises(TypeError) as excinfo:
            data_to_csv(test_data, table_name)

        assert "Input 'data' must be a dictionary." in str(excinfo.value)

    def test_giving_non_list_headers_when_data_to_csv_then_raises_valueerror(self):
        test_data = {
            'headers': "not a list",
            'body': [['row1']]
        }
        table_name = 'test_table'

        with pytest.raises(ValueError) as excinfo:
            data_to_csv(test_data, table_name)

        assert "'headers' and 'body' in 'data' must be lists." in str(excinfo.value)

    def test_giving_non_list_body_when_data_to_csv_then_raises_valueerror(self):
        test_data = {
            'headers': ['col1'],
            'body': "not a list"
        }
        table_name = 'test_table'

        with pytest.raises(ValueError) as excinfo:
            data_to_csv(test_data, table_name)

        assert "'headers' and 'body' in 'data' must be lists." in str(excinfo.value)

    def test_giving_io_error_during_file_write_when_data_to_csv_then_raises_ioerror(self, tmpdir):
        test_data = {
            'headers': ['col1'],
            'body': [['row1']]
        }
        table_name = 'test_table'
        read_only_dir = tmpdir.mkdir("readonly")
        os.chmod(str(read_only_dir), 0o555) # Make directory read-only

        csv_filepath = str(read_only_dir.join(f'{table_name}.csv'))

        with pytest.raises(IOError) as excinfo:
            data_to_csv(test_data, str(read_only_dir.join(table_name)), ) # pass path without .csv, function adds it

        assert "Error writing to CSV file" in str(excinfo.value)

        # os.chmod(str(read_only_dir), 0o777)