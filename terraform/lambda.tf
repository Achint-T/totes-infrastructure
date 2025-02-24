data "archive_file" "ingestion_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/ingestion/function.zip"
  source_file = "${path.module}/../src/lambda_ingest.py"
}

resource "aws_lambda_function" "ingestion_handler" {
  function_name    = "ingestion_handler"
  filename = data.archive_file.ingestion_lambda.output_path
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_ingest.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30
}