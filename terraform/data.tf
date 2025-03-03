data "terraform_remote_state" "s3" {
  backend = "s3"
  config = {
    bucket = "team-mourne-tf-state-bucket"
    key    = "totes-infrastructure/terraform-no-deletion.tfstate"
    region = "eu-west-2"
  }
}

data "aws_iam_policy_document" "s3_document" {
  statement {
    actions = ["s3:PutObject", "s3:GetObject"]
    resources = [
      "${data.terraform_remote_state.s3.outputs.s3_ingestion_bucket_arn}/*",
      "${data.terraform_remote_state.s3.outputs.s3_transform_bucket_arn}/*"
    ]
  }
}

data "aws_s3_bucket" "s3_ingestion_bucket" {
  bucket = "mourne-s3-totes-sys-ingestion-bucket"
}

data "aws_s3_bucket" "s3_transform_bucket" {
  bucket = "mourne-s3-totes-sys-transform-bucket"
}