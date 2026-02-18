# Create deployment package
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_package"
  output_path = "${path.module}/../lambda_function.zip"
}

# Lambda function
resource "aws_lambda_function" "github_issue_extractor" {
  function_name = "github-issue-extractor"
  description   = "Extracts GitHub issues from repository and stores in S3"

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  handler = "lambda_handler.lambda_handler"
  runtime = "python3.11"

  role = aws_iam_role.lambda_role.arn

  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory

  environment {
    variables = {
      GITHUB_TOKEN = var.github_token
      S3_BUCKET    = var.s3_bucket
      REPO_OWNER   = var.repo_owner
      REPO_NAME    = var.repo_name
    }
  }

  tags = {
    Name = "github-issue-extractor"
  }
}

# CloudWatch Log Group with retention
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.github_issue_extractor.function_name}"
  retention_in_days = 14
}
