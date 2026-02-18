"""
AWS Lambda Handler for GitHub Issue Extractor

This is the entry point for the Lambda function.
It imports and uses the GitHubIssueExtractor class.
"""

import json
import os
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from github_issue_extractor import GitHubIssueExtractor


def get_secret(secret_name: str, region_name: str = None) -> str:
    """
    Retrieve secret value from AWS Secrets Manager.

    Args:
        secret_name: Name of the secret in Secrets Manager
        region_name: AWS region (defaults to AWS_REGION env var)

    Returns:
        Secret string value

    Raises:
        Exception: If secret cannot be retrieved
    """
    region = region_name or os.getenv("AWS_REGION", "ap-southeast-2")
    client = boto3.client("secretsmanager", region_name=region)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        # Handle both string and binary secrets
        if "SecretString" in response:
            secret = response["SecretString"]
            # Try to parse as JSON (common pattern)
            try:
                secret_dict = json.loads(secret)
                # If it's a dict, return the token value or the whole secret
                if isinstance(secret_dict, dict):
                    return secret_dict.get(
                        "token", secret_dict.get("GITHUB_TOKEN", secret)
                    )
            except json.JSONDecodeError:
                return secret
            return secret
        else:
            return response["SecretBinary"].decode("utf-8")
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise


def get_config_from_env() -> Dict[str, str]:
    """
    Load configuration from environment variables.

    Returns:
        Dictionary with configuration values
    """
    # Get GitHub token from Secrets Manager
    secret_name = os.getenv("GITHUB_TOKEN_SECRET_NAME", "github/api-token")
    github_token = ""

    try:
        github_token = get_secret(secret_name)
    except Exception as e:
        print(f"Warning: Could not retrieve GitHub token from Secrets Manager: {e}")
        # Fallback to environment variable for local testing
        github_token = os.getenv("GITHUB_TOKEN", "")

    return {
        "github_token": github_token,
        "s3_bucket": os.getenv("S3_BUCKET", "github-api-extraction-bucket"),
        "repo_owner": os.getenv("REPO_OWNER", "pandas-dev"),
        "repo_name": os.getenv("REPO_NAME", "pandas"),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entry point.

    Args:
        event: Lambda event data. Can contain override config:
            - repo_owner: Override repository owner
            - repo_name: Override repository name
            - s3_bucket: Override S3 bucket
        context: Lambda context object

    Returns:
        Dictionary with statusCode and body containing results
    """
    # Load config from environment
    config = get_config_from_env()

    # Allow event-based overrides
    if event:
        if "repo_owner" in event:
            config["repo_owner"] = event["repo_owner"]
        if "repo_name" in event:
            config["repo_name"] = event["repo_name"]
        if "s3_bucket" in event:
            config["s3_bucket"] = event["s3_bucket"]

    # Validate required config
    if not config["github_token"]:
        return {
            "statusCode": 500,
            "body": "Error: Could not retrieve GITHUB_TOKEN from Secrets Manager.",
        }

    try:
        # Create extractor instance
        extractor = GitHubIssueExtractor(
            github_token=config["github_token"],
            s3_bucket=config["s3_bucket"],
            repo_owner=config["repo_owner"],
            repo_name=config["repo_name"],
        )

        # Run extraction
        result = extractor.run_extraction()

        return {"statusCode": 200, "body": result}

    except ValueError as e:
        print(f"Configuration error: {e}")
        return {"statusCode": 400, "body": f"Configuration error: {str(e)}"}
    except Exception as e:
        print(f"Lambda execution failed: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}


# Local testing entry point
if __name__ == "__main__":
    import sys

    # For local testing, load from .env file
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    config = get_config_from_env()

    if not config["github_token"]:
        print("Error: GITHUB_TOKEN not found in environment.")
        sys.exit(1)

    # Run extraction
    result = lambda_handler({}, None)
    print(f"Result: {result}")
