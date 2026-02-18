"""
Unit tests for Lambda Handler.

Run with: pytest tests/test_lambda_handler.py -v
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_handler import lambda_handler, get_config_from_env


class TestGetConfigFromEnv:
    """Tests for get_config_from_env function."""

    def test_reads_env_variables(self):
        """Test reading configuration from environment."""
        with patch.dict(
            os.environ,
            {
                "GITHUB_TOKEN": "test_token",
                "S3_BUCKET": "my-bucket",
                "REPO_OWNER": "my-owner",
                "REPO_NAME": "my-repo",
            },
        ):
            config = get_config_from_env()

        assert config["github_token"] == "test_token"
        assert config["s3_bucket"] == "my-bucket"
        assert config["repo_owner"] == "my-owner"
        assert config["repo_name"] == "my-repo"

    def test_uses_defaults_when_not_set(self):
        """Test using default values when env vars not set."""
        with patch.dict(os.environ, {"GITHUB_TOKEN": "token"}, clear=True):
            config = get_config_from_env()

        assert config["s3_bucket"] == "github-api-extraction-bucket"
        assert config["repo_owner"] == "pandas-dev"
        assert config["repo_name"] == "pandas"


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @patch.dict(os.environ, {}, clear=True)
    def test_returns_error_without_token(self):
        """Test error response when GITHUB_TOKEN not set."""
        result = lambda_handler({}, None)

        assert result["statusCode"] == 500
        assert "GITHUB_TOKEN" in result["body"]

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    def test_creates_extractor_with_config(self, mock_extractor_class):
        """Test that extractor is created with correct config."""
        mock_extractor = Mock()
        mock_extractor.run_extraction.return_value = {"total_saved": 10}
        mock_extractor_class.return_value = mock_extractor

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        mock_extractor_class.assert_called_once()
        mock_extractor.run_extraction.assert_called_once()

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    def test_event_overrides_config(self, mock_extractor_class):
        """Test that event parameters override env config."""
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
    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    def test_returns_500_on_exception(self, mock_extractor_class):
        """Test error handling when extraction fails."""
        mock_extractor_class.side_effect = Exception("Unexpected error")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 500
        assert "Unexpected error" in result["body"]

    @patch("lambda_handler.GitHubIssueExtractor")
    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    def test_returns_400_on_value_error(self, mock_extractor_class):
        """Test handling of configuration errors."""
        mock_extractor_class.side_effect = ValueError("Invalid config")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 400
        assert "Configuration error" in result["body"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
