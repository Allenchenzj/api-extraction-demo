variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "ap-southeast-2"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "github_token_secret_name" {
  description = "Name of the AWS Secrets Manager secret containing GitHub token"
  type        = string
  default     = "github/api-token"
}

variable "s3_bucket" {
  description = "S3 bucket for storing extracted data and state"
  type        = string
  default     = "github-api-extraction-bucket"
}

variable "repo_owner" {
  description = "GitHub repository owner"
  type        = string
  default     = "pandas-dev"
}

variable "repo_name" {
  description = "GitHub repository name"
  type        = string
  default     = "pandas"
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300 # 5 minutes
}

variable "lambda_memory" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 512
}
