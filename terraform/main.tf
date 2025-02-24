terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "team-mourne-tf-state-bucket"
    key = "totes-infrastructure/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "TotesSys Data Warehouse"
      Team = "Team Mourne"
      DeployedFrom = "Terraform"
      Repository = "totes-infrastructure"
      Environment = "dev"
    }
  }
}