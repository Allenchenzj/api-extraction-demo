"""
AWS Lambda Handler for GitHub Issue Extractor

This is the entry point for the Lambda function.
It imports and uses the GitHubIssueExtractor class.
"""

import os
from typing import Any, Dict

from github_issue_extractor import GitHubIssueExtractor


def get_config_from_env() -> Dict[str, str]:
    """
    Load configuration from environment variables.

    Returns:
        Dictionary with configuration values
    """
    return {
        "github_token": os.getenv("GITHUB_TOKEN", ""),
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
            "body": "Error: GITHUB_TOKEN environment variable not set.",
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
