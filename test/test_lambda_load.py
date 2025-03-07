# import pytest
# from unittest.mock import MagicMock, patch
# from src.lambda_load import lambda_handler
# import os

# class TestLambdaHandler:
#     @pytest.fixture(autouse=True)
#     def env_variable(self):
#         variable_name = "BUCKET_NAME"
#         variable_value = "DEMO_BUCKET"
#         os.environ[variable_name] = variable_value

#         yield

#         if variable_name in os.environ:
#             del os.environ[variable_name]

#     @pytest.fixture
#     def mock_event(self):
#         return {
#             "status_code": 200,
#             "fact_tables": {"fact_table_1": "s3_key_fact_1"},
#             "dim_tables": {"dim_table_1": "s3_key_dim_1"}
#         }

#     @pytest.fixture
#     def mock_context(self, env_variable):
#         return MagicMock()

#     # @patch("src.load_utils.write_dataframe_to_dw.process_fact_tables")
#     # @patch("src.load_utils.write_dataframe_to_dw.process_dim_tables")
#     # def test_giving_valid_event_when_lambda_handler_then_call_process_fact_and_dim_tables(self, mock_process_dim, mock_process_fact, mock_event, mock_context):
#     #     lambda_handler(mock_event, mock_context)
#     #     mock_process_fact.assert_called_once()
#     #     mock_process_dim.assert_called_once()

#     # @patch("src.load_utils.write_dataframe_to_dw.process_fact_tables")
#     # @patch("src.load_utils.write_dataframe_to_dw.process_dim_tables")
#     # def test_giving_valid_event_when_lambda_handler_then_return_success_response(self, mock_process_dim, mock_process_fact, mock_event, mock_context):
#     #     response = lambda_handler(mock_event, mock_context)
#     #     assert response == {"statusCode": 200, "body": "Data load completed"}

#     def test_giving_invalid_event_type_when_lambda_handler_then_raise_typeerror(self, mock_context):
#         with pytest.raises(TypeError):
#             lambda_handler("not_a_dict", mock_context)

#     def test_giving_event_missing_fact_tables_when_lambda_handler_then_raise_valueerror(self, mock_event, mock_context):
#         del mock_event["fact_tables"]
#         with pytest.raises(ValueError):
#             lambda_handler(mock_event, mock_context)

#     def test_giving_event_missing_dim_tables_when_lambda_handler_then_raise_valueerror(self, mock_event, mock_context):
#         del mock_event["dim_tables"]
#         with pytest.raises(ValueError):
#             lambda_handler(mock_event, mock_context)

#     # @patch("src.load_utils.write_dataframe_to_dw.process_fact_tables", side_effect=Exception("Fact table processing failed"))
#     # @patch("src.load_utils.write_dataframe_to_dw.process_dim_tables")
#     # def test_giving_exception_in_process_fact_tables_when_lambda_handler_then_return_error_response(self, mock_process_dim, mock_process_fact, mock_event, mock_context):
#     #     response = lambda_handler(mock_event, mock_context)
#     #     assert response["statusCode"] == 500
#     #     assert "Error processing data load" in response["body"]

#     # @patch("src.load_utils.write_dataframe_to_dw.process_fact_tables")
#     # @patch("src.load_utils.write_dataframe_to_dw.process_dim_tables", side_effect=Exception("Dim table processing failed"))
#     # def test_giving_exception_in_process_dim_tables_when_lambda_handler_then_return_error_response(self, mock_process_dim, mock_process_fact, mock_event, mock_context):
#     #     response = lambda_handler(mock_event, mock_context)
#     #     assert response["statusCode"] == 500
#     #     assert "Error processing data load" in response["body"]