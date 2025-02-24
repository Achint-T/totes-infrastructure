resource "aws_s3_bucket" "s3_ingestion_bucket" {
    bucket_prefix = "${var.S3_BUCKET_PREFIX}-"
}

resource "aws_s3_bucket" "s3_ingestion_bucket" {
    bucket_prefix = "test-s3-bucket-for-totes-infra"
}