"""
Unit tests for Lambda Handler.

Run with: pytest tests/test_lambda_handler.py -v
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_handler import lambda_handler, get_config_from_env, get_secret


class TestGetSecret:
    """Tests for get_secret function."""

    @patch("lambda_handler.boto3.client")
    def test_retrieves_plain_string_secret(self, mock_boto_client):
        """Test retrieving a plain string secret."""
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": "ghp_test_token_123"
        }
        mock_boto_client.return_value = mock_client

        result = get_secret("github/api-token")

        assert result == "ghp_test_token_123"
        mock_client.get_secret_value.assert_called_once_with(
            SecretId="github/api-token"
        )

    @patch("lambda_handler.boto3.client")
    def test_retrieves_json_secret_with_token_key(self, mock_boto_client):
        """Test retrieving a JSON secret with 'token' key."""
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": '{"token": "ghp_json_token"}'
        }
        mock_boto_client.return_value = mock_client

        result = get_secret("github/api-token")

        assert result == "ghp_json_token"

    @patch("lambda_handler.boto3.client")
    def test_retrieves_json_secret_with_github_token_key(self, mock_boto_client):
        """Test retrieving a JSON secret with 'GITHUB_TOKEN' key."""
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": '{"GITHUB_TOKEN": "ghp_github_token"}'
        }
        mock_boto_client.return_value = mock_client

        result = get_secret("github/api-token")

        assert result == "ghp_github_token"


class TestGetConfigFromEnv:
    """Tests for get_config_from_env function."""

    @patch("lambda_handler.get_secret")
    def test_reads_token_from_secrets_manager(self, mock_get_secret):
        """Test reading GitHub token from Secrets Manager."""
        mock_get_secret.return_value = "secret_token"

        with patch.dict(
            os.environ,
            {
                "GITHUB_TOKEN_SECRET_NAME": "github/api-token",
                "S3_BUCKET": "my-bucket",
                "REPO_OWNER": "my-owner",
                "REPO_NAME": "my-repo",
            },
        ):
            config = get_config_from_env()

        assert config["github_token"] == "secret_token"
        assert config["s3_bucket"] == "my-bucket"
        assert config["repo_owner"] == "my-owner"
        assert config["repo_name"] == "my-repo"
        mock_get_secret.assert_called_once_with("github/api-token")

    @patch("lambda_handler.get_secret")
    def test_falls_back_to_env_var_on_secret_error(self, mock_get_secret):
        """Test fallback to GITHUB_TOKEN env var when Secrets Manager fails."""
        mock_get_secret.side_effect = Exception("Secret not found")

        with patch.dict(
            os.environ,
            {
                "GITHUB_TOKEN": "fallback_token",
            },
            clear=True,
        ):
            config = get_config_from_env()

        assert config["github_token"] == "fallback_token"

    @patch("lambda_handler.get_secret")
    def test_uses_defaults_when_not_set(self, mock_get_secret):
        """Test using default values when env vars not set."""
        mock_get_secret.return_value = "token"

        with patch.dict(os.environ, {}, clear=True):
            config = get_config_from_env()

        assert config["s3_bucket"] == "github-api-extraction-bucket"
        assert config["repo_owner"] == "pandas-dev"
        assert config["repo_name"] == "pandas"


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @patch("lambda_handler.get_secret")
    @patch.dict(os.environ, {}, clear=True)
    def test_returns_error_without_token(self, mock_get_secret):
        """Test error response when token cannot be retrieved."""
        mock_get_secret.side_effect = Exception("Secret not found")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 500
        assert "Secrets Manager" in result["body"]

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch("lambda_handler.get_secret")
    def test_creates_extractor_with_config(self, mock_get_secret, mock_extractor_class):
        """Test that extractor is created with correct config."""
        mock_get_secret.return_value = "test_token"
        mock_extractor = Mock()
        mock_extractor.run_extraction.return_value = {"total_saved": 10}
        mock_extractor_class.return_value = mock_extractor

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        mock_extractor_class.assert_called_once()
        mock_extractor.run_extraction.assert_called_once()

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch("lambda_handler.get_secret")
    def test_event_overrides_config(self, mock_get_secret, mock_extractor_class):
        """Test that event parameters override env config."""
        mock_get_secret.return_value = "test_token"
        mock_extractor = Mock()
        mock_extractor.run_extraction.return_value = {"total_saved": 5}
        mock_extractor_class.return_value = mock_extractor

        event = {
            "repo_owner": "custom-owner",
            "repo_name": "custom-repo",
            "s3_bucket": "custom-bucket",
        }

        lambda_handler(event, None)

        call_kwargs = mock_extractor_class.call_args[1]
        assert call_kwargs["repo_owner"] == "custom-owner"
        assert call_kwargs["repo_name"] == "custom-repo"
        assert call_kwargs["s3_bucket"] == "custom-bucket"

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch("lambda_handler.get_secret")
    def test_returns_500_on_exception(self, mock_get_secret, mock_extractor_class):
        """Test error handling when extraction fails."""
        mock_get_secret.return_value = "test_token"
        mock_extractor_class.side_effect = Exception("Unexpected error")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 500
        assert "Unexpected error" in result["body"]

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch("lambda_handler.get_secret")
    def test_returns_400_on_value_error(self, mock_get_secret, mock_extractor_class):
        """Test handling of configuration errors."""
        mock_get_secret.return_value = "test_token"
        mock_extractor_class.side_effect = ValueError("Invalid config")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 400
        assert "Configuration error" in result["body"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
