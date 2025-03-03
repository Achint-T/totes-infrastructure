resource "aws_s3_bucket" "s3_ingestion_bucket" {
    bucket_prefix = var.S3_BUCKET_PREFIX
    force_destroy = true
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "totesys-code-bucket-"
}

resource "aws_s3_bucket" "s3_transform_bucket" {
  bucket_prefix = "s3-totes-sys-transform-bucket-"
}