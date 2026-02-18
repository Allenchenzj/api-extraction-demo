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

  # Backend configuration for state storage
  # Uncomment and configure if you want remote state
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "github-issue-extractor/terraform.tfstate"
  #   region         = "ap-southeast-2"
  #   encrypt        = true
  # }
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
