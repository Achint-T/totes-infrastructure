resource "aws_s3_bucket" "s3_ingestion_bucket" {
    bucket = "mourne-s3-totes-sys-ingestion-bucket-"
    object_lock_enabled = true
}

resource "aws_s3_bucket" "s3_transform_bucket" {
  bucket_prefix = "mourne-s3-totes-sys-transform-bucket-"
  object_lock_enabled = true
}

output "s3_ingestion_bucket_arn" {
  value = aws_s3_bucket.s3_ingestion_bucket.arn
}

output "s3_transform_bucket_arn" {
  value = aws_s3_bucket.s3_transform_bucket.arn
}