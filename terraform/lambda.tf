data "archive_file" "ingestion_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/ingestion/function.zip"
  source_file = "${path.module}/../src/lambda_ingest.py"
}

resource "null_resource" "prepare_layer_files" {
  triggers = {
    
    helper_file_hash = filebase64sha256("${path.module}/../src/ingestion_utils/database_utils.py")
    helper_file_hash = filebase64sha256("${path.module}/../src/ingestion_utils/file_utils.py")
    helper_file_hash = filebase64sha256("${path.module}/../src/helpers.py")
    
}

  provisioner "local-exec" {
    command = <<EOT
    LAYER_PATH="${path.module}/../packages/ingestion/layer/python/lib/python3.12/site-packages"
      mkdir -p "$LAYER_PATH"
      cp "${path.module}/../src/helpers.py" "$LAYER_PATH/helpers.py"
      cp "${path.module}/../src/ingestion_utils/database_utils.py" "$LAYER_PATH/database_utils.py"
      cp "${path.module}/../src/ingestion_utils/file_utils.py" "$LAYER_PATH/file_utils.py"

      pip install --no-cache-dir pg8000 --target "$LAYER_PATH"
    EOT
  }
}

data "archive_file" "helper_lambda_layer" {
  type        = "zip"
  output_path = "${path.module}/../packages/ingestion/helpers.zip"
  source_dir = "${path.module}/../packages/ingestion/layer"
  depends_on = [ null_resource.prepare_layer_files ]
}

resource "aws_s3_object" "lambda_code" {
bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingestion/function.zip"
  source = data.archive_file.ingestion_lambda.output_path
  etag   = filemd5(data.archive_file.ingestion_lambda.output_path)
}

resource "aws_s3_object" "helper_layer_code" {
bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingestion/helpers.zip"
  source = data.archive_file.helper_lambda_layer.output_path
}

resource "aws_lambda_layer_version" "helper_lambda_layer" {
  layer_name          = "helpers-layer" 
  s3_bucket           = aws_s3_bucket.code_bucket.id
  s3_key              = aws_s3_object.helper_layer_code.key
  s3_object_version   = aws_s3_object.helper_layer_code.version_id
}

resource "aws_lambda_function" "ingestion_handler" {
  function_name    = "ingestion_handler"
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = aws_s3_object.lambda_code.key
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.helper_lambda_layer.arn]
  handler          = "lambda_ingest.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30
}


# Transform lambda 

data "archive_file" "transform_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/transform/function.zip"
  source_file = "${path.module}/../src/lambda_transform.py"
}

resource "aws_s3_object" "transform_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "transform/function.zip"
  source = data.archive_file.transform_lambda.output_path
  etag   = filemd5(data.archive_file.transform_lambda.output_path)
}

resource "aws_lambda_function" "transform_handler" {
  function_name    = "transform_handler"
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = aws_s3_object.lambda_code.key
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.helper_lambda_layer.arn]
  handler          = "lambda_transform.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30
}

