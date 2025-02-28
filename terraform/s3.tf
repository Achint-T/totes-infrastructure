resource "aws_s3_bucket" "s3_ingestion_bucket" {
    bucket = "mourne-s3-totes-sys-ingestion-bucket-2"
    object_lock_enabled = true
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "totesys-code-bucket-"
}

resource "aws_s3_bucket" "s3_transform_bucket" {
  bucket_prefix = "s3-totes-sys-transform-bucket-"
}