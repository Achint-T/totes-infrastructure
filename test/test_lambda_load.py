# import pytest
# from unittest.mock import patch, MagicMock
# from src.lambda_load import lambda_handler

# class TestLambdaHandler:

#     @pytest.fixture
#     def mock_boto3_client(self):
#         with patch("src.lambda_load.boto3.client") as mock_client:
#             yield mock_client

#     @pytest.fixture
#     def mock_get_connection(self):
#         with patch("src.lambda_load.get_connection") as mock_conn:
#             mock_db_conn = MagicMock()
#             mock_conn.return_value = mock_db_conn
#             yield mock_conn, mock_db_conn

#     @pytest.fixture
#     def mock_process_tables(self):
#         with patch("src.lambda_load.process_tables") as mock_process:
#             yield mock_process

#     def test_giving_invalid_event_type_when_lambda_handler_is_called_then_raise_type_error(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = "not a dict"
#         with pytest.raises(TypeError) as exc_info:
#             lambda_handler(event, None)
#         assert str(exc_info.value) == "event must be a dictionary"

#     def test_giving_event_without_fact_tables_when_lambda_handler_is_called_then_raise_value_error(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {"dim_tables": {"table1": "s3_key1"}}
#         with pytest.raises(ValueError) as exc_info:
#             lambda_handler(event, None)
#         assert str(exc_info.value) == "event must contain 'fact_tables'"

#     def test_giving_event_without_dim_tables_when_lambda_handler_is_called_then_raise_value_error(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {"fact_tables": {"table1": "s3_key1"}}
#         with pytest.raises(ValueError) as exc_info:
#             lambda_handler(event, None)
#         assert str(exc_info.value) == "event must contain 'dim_tables'"

#     def test_giving_valid_event_with_fact_and_dim_tables_when_lambda_handler_is_called_then_return_success_response(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {
#             "fact_tables": {"fact_table1": "s3_fact_key1"},
#             "dim_tables": {"dim_table1": "s3_dim_key1"}
#         }
#         response = lambda_handler(event, None)
#         assert response == {"status_code": 200, "body": "Data load completed"}
#         mock_get_connection[0].assert_called_once()
#         mock_process_tables.assert_called()
#         assert mock_process_tables.call_count == 2
#         mock_process_tables.assert_any_call({"dim_table1": "s3_dim_key1"}, mock_boto3_client.return_value, mock_get_connection[1], is_fact=False)
#         mock_process_tables.assert_any_call({"fact_table1": "s3_fact_key1"}, mock_boto3_client.return_value, mock_get_connection[1], is_fact=True)
#         mock_get_connection[1].close.assert_called_once()


#     def test_giving_process_tables_raises_exception_for_dim_tables_when_lambda_handler_is_called_then_return_error_response(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {
#             "fact_tables": {"fact_table1": "s3_fact_key1"},
#             "dim_tables": {"dim_table1": "s3_dim_key1"}
#         }
#         mock_process_tables.side_effect = Exception("Error processing dim tables")
#         response = lambda_handler(event, None)
#         assert response == {'status_code': 500, 'body': 'Error processing data load: Error processing dim tables'}
#         mock_get_connection[0].assert_called_once()
#         mock_process_tables.assert_called_once() # It will fail on first call for dim_tables
#         mock_get_connection[1].close.assert_called_once()

#     def test_giving_process_tables_raises_exception_for_fact_tables_when_lambda_handler_is_called_then_return_error_response(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {
#             "fact_tables": {"fact_table1": "s3_fact_key1"},
#             "dim_tables": {"dim_table1": "s3_dim_key1"}
#         }
#         mock_process_tables.side_effect = [None, Exception("Error processing fact tables")] # first call ok, second call exception
#         response = lambda_handler(event, None)
#         assert response == {'status_code': 500, 'body': 'Error processing data load: Error processing fact tables'}
#         mock_get_connection[0].assert_called_once()
#         assert mock_process_tables.call_count == 2
#         mock_get_connection[1].close.assert_called_once()

#     def test_giving_get_connection_raises_exception_when_lambda_handler_is_called_then_return_error_response(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {
#             "fact_tables": {"fact_table1": "s3_fact_key1"},
#             "dim_tables": {"dim_table1": "s3_dim_key1"}
#         }
#         mock_get_connection[0].side_effect = Exception("Error connecting to DB")
#         response = lambda_handler(event, None)
#         assert response == {'status_code': 500, 'body': 'Error processing data load: Error connecting to DB'}
#         mock_get_connection[0].assert_called_once()
#         mock_process_tables.assert_not_called() # process_tables should not be called if connection fails
#         mock_get_connection[1].close.assert_not_called() # close should not be called if connection fails

#     def test_giving_valid_event_when_lambda_handler_is_called_then_input_event_is_not_modified(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         initial_event = {
#             "fact_tables": {"fact_table1": "s3_fact_key1"},
#             "dim_tables": {"dim_table1": "s3_dim_key1"}
#         }
#         event = initial_event.copy() # create a copy to check for modification
#         lambda_handler(event, None)
#         mock_get_connection[1].close.assert_called_once()
#         assert event == initial_event # check if original event is modified

#     def test_giving_valid_event_with_only_fact_tables_when_lambda_handler_is_called_then_return_success_response(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {
#             "fact_tables": {"fact_table1": "s3_fact_key1"},
#             "dim_tables": {}
#         }
#         response = lambda_handler(event, None)
#         assert response == {"status_code": 200, "body": "Data load completed"}
#         mock_get_connection[0].assert_called_once()
#         mock_process_tables.assert_called()
#         assert mock_process_tables.call_count == 2
#         mock_process_tables.assert_any_call({}, mock_boto3_client.return_value, mock_get_connection[1], is_fact=False)
#         mock_process_tables.assert_any_call({"fact_table1": "s3_fact_key1"}, mock_boto3_client.return_value, mock_get_connection[1], is_fact=True)
#         mock_get_connection[1].close.assert_called_once()

#     def test_giving_valid_event_with_only_dim_tables_when_lambda_handler_is_called_then_return_success_response(self, mock_boto3_client, mock_get_connection, mock_process_tables):
#         event = {
#             "fact_tables": {},
#             "dim_tables": {"dim_table1": "s3_dim_key1"}
#         }
#         response = lambda_handler(event, None)
#         assert response == {"status_code": 200, "body": "Data load completed"}
#         mock_get_connection[0].assert_called_once()
#         mock_process_tables.assert_called()
#         assert mock_process_tables.call_count == 2
#         mock_process_tables.assert_any_call({"dim_table1": "s3_dim_key1"}, mock_boto3_client.return_value, mock_get_connection[1], is_fact=False)
#         mock_process_tables.assert_any_call({}, mock_boto3_client.return_value, mock_get_connection[1], is_fact=True)