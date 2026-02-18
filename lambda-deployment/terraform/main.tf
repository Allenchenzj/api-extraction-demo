terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }

  # S3 backend for persistent state across CI/CD runs
  backend "s3" {
    bucket  = "github-api-extraction-bucket"
    key     = "terraform/github-issue-extractor.tfstate"
    region  = "ap-southeast-2"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "github-issue-extractor"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
