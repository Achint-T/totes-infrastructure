data "archive_file" "ingestion_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/ingestion/function.zip"
  source_file = "${path.module}/../src/lambda_ingest.py"
}

resource "null_resource" "prepare_layer_files" {
  triggers = {
    
    helper_file_hash_1 = filebase64sha256("${path.module}/../src/ingestion_utils/database_utils.py")
    helper_file_hash_2 = filebase64sha256("${path.module}/../src/ingestion_utils/file_utils.py")
    helper_file_hash_3 = filebase64sha256("${path.module}/../src/helpers.py")
    
}

  provisioner "local-exec" {
    command = <<EOT
    LAYER_PATH="${path.module}/../packages/ingestion/layer/python/lib/python3.12/site-packages"
      mkdir -p "$LAYER_PATH"
      mkdir -p "$LAYER_PATH/ingestion_utils"
      cp "${path.module}/../src/helpers.py" "$LAYER_PATH/helpers.py"
      cp "${path.module}/../src/ingestion_utils/database_utils.py" "$LAYER_PATH/ingestion_utils/database_utils.py"
      cp "${path.module}/../src/ingestion_utils/file_utils.py" "$LAYER_PATH/ingestion_utils/file_utils.py"

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
  #etag   = filemd5(data.archive_file.helper_lambda_layer.output_path)
  depends_on = [ null_resource.prepare_layer_files ]
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
  timeout          = 60

  environment {
    variables = {
      BUCKET_NAME = data.aws_s3_bucket.s3_ingestion_bucket.bucket
    }
  }
}

# Transform lambda 


resource "null_resource" "prepare_layer_files_transform" {
  triggers = {
    
    helper_file_hash_1 = filebase64sha256("${path.module}/../src/transform_utils/dim_counterparty.py")
    helper_file_hash_2 = filebase64sha256("${path.module}/../src/transform_utils/dim_currency.py")
    helper_file_hash_3 = filebase64sha256("${path.module}/../src/helpers.py")
    helper_file_hash_4 = filebase64sha256("${path.module}/../src/transform_utils/dim_date.py")
    helper_file_hash_5 = filebase64sha256("${path.module}/../src/transform_utils/dim_design.py")
    helper_file_hash_6 = filebase64sha256("${path.module}/../src/transform_utils/dim_location.py")
    helper_file_hash_7 = filebase64sha256("${path.module}/../src/transform_utils/dim_staff.py")
    helper_file_hash_8 = filebase64sha256("${path.module}/../src/transform_utils/fact_sales_order.py")
    helper_file_hash_9 = filebase64sha256("${path.module}/../src/transform_utils/file_utils.py")
    helper_file_hash_10 = filebase64sha256("${path.module}/../src/transform_utils/fact_payment.py")
    helper_file_hash_11 = filebase64sha256("${path.module}/../src/transform_utils/dim_transaction.py")
    helper_file_hash_12 = filebase64sha256("${path.module}/../src/transform_utils/dim_payment_type.py")
    helper_file_hash_13 = filebase64sha256("${path.module}/../src/transform_utils/fact_purchase_order.py")
    
}

  provisioner "local-exec" {
    command = <<EOT
    LAYER_PATH="${path.module}/../packages/transform/layer/python/lib/python3.12/site-packages"
      mkdir -p "$LAYER_PATH"
      mkdir -p "$LAYER_PATH/transform_utils"
      cp "${path.module}/../src/helpers.py" "$LAYER_PATH/helpers.py"
      cp "${path.module}/../src/transform_utils/dim_counterparty.py" "$LAYER_PATH/transform_utils/dim_counterparty.py"
      cp "${path.module}/../src/transform_utils/dim_currency.py" "$LAYER_PATH/transform_utils/dim_currency.py"
      cp "${path.module}/../src/transform_utils/dim_date.py" "$LAYER_PATH/transform_utils/dim_date.py"
      cp "${path.module}/../src/transform_utils/dim_design.py" "$LAYER_PATH/transform_utils/dim_design.py"
      cp "${path.module}/../src/transform_utils/dim_location.py" "$LAYER_PATH/transform_utils/dim_location.py"
      cp "${path.module}/../src/transform_utils/dim_staff.py" "$LAYER_PATH/transform_utils/dim_staff.py"
      cp "${path.module}/../src/transform_utils/fact_sales_order.py" "$LAYER_PATH/transform_utils/fact_sales_order.py"
      cp "${path.module}/../src/transform_utils/file_utils.py" "$LAYER_PATH/transform_utils/file_utils.py"
      cp "${path.module}/../src/transform_utils/fact_payment.py" "$LAYER_PATH/transform_utils/fact_payment.py"
      cp "${path.module}/../src/transform_utils/dim_transaction.py" "$LAYER_PATH/transform_utils/dim_transaction.py"
      cp "${path.module}/../src/transform_utils/dim_payment_type.py" "$LAYER_PATH/transform_utils/dim_payment_type.py"
      cp "${path.module}/../src/transform_utils/fact_purchase_order.py" "$LAYER_PATH/transform_utils/fact_purchase_order.py"

    EOT
  }
}

data "archive_file" "transform_lambda_layer_arch" {
  type        = "zip"
  output_path = "${path.module}/../packages/transform/helpers.zip"
  source_dir = "${path.module}/../packages/transform/layer"
  depends_on = [ null_resource.prepare_layer_files_transform ]
}

resource "aws_s3_object" "transform_layer_code" {
bucket = aws_s3_bucket.code_bucket.bucket
  key    = "transform/helpers.zip"
  source = data.archive_file.transform_lambda_layer_arch.output_path
  #etag   = filemd5(data.archive_file.transform_lambda_layer_arch.output_path)
  depends_on = [ null_resource.prepare_layer_files_transform ]
}

resource "aws_lambda_layer_version" "transform_lambda_layer" {
  layer_name          = "transform-layer" 
  s3_bucket           = aws_s3_bucket.code_bucket.id
  s3_key              = aws_s3_object.transform_layer_code.key
  s3_object_version   = aws_s3_object.transform_layer_code.version_id
}

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
  s3_key           = aws_s3_object.transform_lambda_code.key
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.transform_lambda_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16"] #attach aws pandas layer with arn
  handler          = "lambda_transform.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60


environment {
    variables = {
      INGESTION_BUCKET_NAME = data.aws_s3_bucket.s3_ingestion_bucket.bucket
      TRANSFORMED_BUCKET_NAME = data.aws_s3_bucket.s3_transform_bucket.bucket
    }
  }
}

# Load Lambda

resource "null_resource" "prepare_layer_files_load" {
  triggers = {
    
    helper_file_hash_1 = filebase64sha256("${path.module}/../src/load_utils/read_parquet.py")
    helper_file_hash_2 = filebase64sha256("${path.module}/../src/load_utils/write_dataframe_to_dw.py")
    helper_file_hash_2 = filebase64sha256("${path.module}/../src/helpers.py")
    
}

  provisioner "local-exec" {
    command = <<EOT
    LAYER_PATH="${path.module}/../packages/load/layer/python/lib/python3.12/site-packages"
      mkdir -p "$LAYER_PATH"
      mkdir -p "$LAYER_PATH/load_utils"
      cp "${path.module}/../src/helpers.py" "$LAYER_PATH/helpers.py"
      cp "${path.module}/../src/load_utils/read_parquet.py" "$LAYER_PATH/load_utils/read_parquet.py"
      cp "${path.module}/../src/load_utils/write_dataframe_to_dw.py" "$LAYER_PATH/load_utils/write_dataframe_to_dw.py"

    EOT
  }
}

data "archive_file" "load_lambda_layer_arch" {
  type        = "zip"
  output_path = "${path.module}/../packages/load/helpers.zip"
  source_dir = "${path.module}/../packages/load/layer"
  depends_on = [ null_resource.prepare_layer_files_load ]
}

resource "aws_s3_object" "load_layer_code" {
bucket = aws_s3_bucket.code_bucket.bucket
  key    = "load/helpers.zip"
  source = data.archive_file.load_lambda_layer_arch.output_path
  #etag   = filemd5(data.archive_file.load_lambda_layer_arch.output_path)
  depends_on = [ null_resource.prepare_layer_files_load ]
}

resource "aws_lambda_layer_version" "load_lambda_layer" {
  layer_name          = "load-layer" 
  s3_bucket           = aws_s3_bucket.code_bucket.id
  s3_key              = aws_s3_object.load_layer_code.key
  s3_object_version   = aws_s3_object.load_layer_code.version_id
}

data "archive_file" "load_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/load/function.zip"
  source_file = "${path.module}/../src/lambda_load.py"
}

resource "aws_s3_object" "load_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "load/function.zip"
  source = data.archive_file.load_lambda.output_path
  etag   = filemd5(data.archive_file.transform_lambda.output_path)
}

resource "aws_lambda_function" "load_handler" {
  function_name    = "load_handler"
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = aws_s3_object.load_lambda_code.key
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.load_lambda_layer.arn,"arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16"] #attach aws pandas layer with arn]
  handler          = "lambda_load.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60

  environment {
    variables = {
      BUCKET_NAME = data.aws_s3_bucket.s3_transform_bucket.bucket
    }
  }
}