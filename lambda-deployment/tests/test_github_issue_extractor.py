"""
Unit tests for GitHubIssueExtractor class.

Run with: pytest tests/test_github_issue_extractor.py -v
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO

# Add parent directory to path for imports
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from github_issue_extractor import GitHubIssueExtractor


class TestGitHubIssueExtractorInit:
    """Tests for GitHubIssueExtractor initialization."""

    def test_init_with_valid_params(self):
        """Test successful initialization with valid parameters."""
        mock_s3 = Mock()
        extractor = GitHubIssueExtractor(
            github_token="test_token",
            s3_bucket="test-bucket",
            repo_owner="test-owner",
            repo_name="test-repo",
            s3_client=mock_s3,
        )

        assert extractor.github_token == "test_token"
        assert extractor.s3_bucket == "test-bucket"
        assert extractor.repo_owner == "test-owner"
        assert extractor.repo_name == "test-repo"
        assert extractor.s3_client == mock_s3

    def test_init_with_default_values(self):
        """Test initialization with default repo values."""
        mock_s3 = Mock()
        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        assert extractor.repo_owner == "pandas-dev"
        assert extractor.repo_name == "pandas"

    def test_init_without_token_raises_error(self):
        """Test that missing token raises ValueError."""
        with pytest.raises(ValueError, match="github_token is required"):
            GitHubIssueExtractor(github_token="", s3_bucket="test-bucket")

    def test_init_without_bucket_raises_error(self):
        """Test that missing bucket raises ValueError."""
        with pytest.raises(ValueError, match="s3_bucket is required"):
            GitHubIssueExtractor(github_token="test_token", s3_bucket="")


class TestGetLastSyncTime:
    """Tests for get_last_sync_time method."""

    def test_returns_timestamp_from_s3(self):
        """Test reading existing state from S3."""
        mock_s3 = Mock()
        state_data = {"last_updated": "2024-06-15T10:30:00Z"}
        mock_s3.get_object.return_value = {
            "Body": BytesIO(json.dumps(state_data).encode("utf-8"))
        }

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        result = extractor.get_last_sync_time()

        assert result == "2024-06-15T10:30:00Z"
        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="issue_state.json"
        )

    def test_returns_default_when_no_state_file(self):
        """Test returning default when state file doesn't exist."""
        mock_s3 = Mock()
        mock_s3.exceptions = Mock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey()

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        result = extractor.get_last_sync_time()

        assert result == GitHubIssueExtractor.DEFAULT_START_DATE

    def test_returns_default_on_s3_error(self):
        """Test returning default on S3 read error."""
        mock_s3 = Mock()
        mock_s3.exceptions = Mock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = Exception("S3 error")

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        result = extractor.get_last_sync_time()

        assert result == GitHubIssueExtractor.DEFAULT_START_DATE


class TestSaveLastSyncTime:
    """Tests for save_last_sync_time method."""

    def test_saves_timestamp_to_s3(self):
        """Test saving timestamp to S3."""
        mock_s3 = Mock()

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        result = extractor.save_last_sync_time("2024-06-15T12:00:00Z")

        assert result is True
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "issue_state.json"
        assert "2024-06-15T12:00:00Z" in call_kwargs["Body"].decode("utf-8")

    def test_returns_false_on_s3_error(self):
        """Test returning False on S3 write error."""
        mock_s3 = Mock()
        mock_s3.put_object.side_effect = Exception("S3 error")

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        result = extractor.save_last_sync_time("2024-06-15T12:00:00Z")

        assert result is False


class TestFlushBufferToCsv:
    """Tests for flush_buffer_to_csv method."""

    def test_uploads_csv_to_s3(self):
        """Test uploading CSV data to S3."""
        mock_s3 = Mock()

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        data = [
            {
                "id": 1,
                "number": 100,
                "title": "Test Issue",
                "user": {"login": "testuser"},
                "state": "open",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-02T00:00:00Z",
                "body": "Test body",
                "html_url": "https://github.com/test/repo/issues/100",
            }
        ]

        result = extractor.flush_buffer_to_csv(data)

        assert result == 1
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"].startswith("data/issues_")
        assert call_kwargs["Key"].endswith("_batch_001.csv")
        assert call_kwargs["ContentType"] == "text/csv"

    def test_extracts_user_login(self):
        """Test that user dict is converted to login string."""
        mock_s3 = Mock()

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        data = [{"id": 1, "user": {"login": "testuser", "id": 123}}]

        extractor.flush_buffer_to_csv(data)

        # Check CSV content contains just the login
        call_kwargs = mock_s3.put_object.call_args[1]
        csv_content = call_kwargs["Body"].decode("utf-8")
        assert "testuser" in csv_content
        assert "123" not in csv_content  # user id should not be in CSV

    def test_returns_zero_for_empty_buffer(self):
        """Test returning 0 for empty data buffer."""
        mock_s3 = Mock()

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        result = extractor.flush_buffer_to_csv([])

        assert result == 0
        mock_s3.put_object.assert_not_called()

    def test_returns_zero_on_s3_error(self):
        """Test returning 0 on S3 upload error."""
        mock_s3 = Mock()
        mock_s3.put_object.side_effect = Exception("Upload failed")

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        data = [{"id": 1, "title": "Test"}]
        result = extractor.flush_buffer_to_csv(data)

        assert result == 0


class TestFetchPageData:
    """Tests for fetch_page_data method."""

    @patch("github_issue_extractor.requests.get")
    def test_successful_fetch(self, mock_get):
        """Test successful page fetch from GitHub API."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": 1, "title": "Issue 1"},
            {"id": 2, "title": "Issue 2"},
        ]
        mock_get.return_value = mock_response

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=Mock()
        )

        result = extractor.fetch_page_data(1, "2024-01-01T00:00:00Z")

        assert len(result) == 2
        assert result[0]["id"] == 1
        mock_get.assert_called_once()

    @patch("github_issue_extractor.requests.get")
    def test_returns_empty_list_on_404(self, mock_get):
        """Test returning empty list on 404 response."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=Mock()
        )

        result = extractor.fetch_page_data(100, "2024-01-01T00:00:00Z")

        assert result == []

    @patch("github_issue_extractor.requests.get")
    def test_returns_empty_list_on_422(self, mock_get):
        """Test returning empty list on 422 response (pagination limit)."""
        mock_response = Mock()
        mock_response.status_code = 422
        mock_get.return_value = mock_response

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=Mock()
        )

        result = extractor.fetch_page_data(100, "2024-01-01T00:00:00Z")

        assert result == []

    @patch("github_issue_extractor.requests.get")
    @patch("github_issue_extractor.time.sleep")
    def test_retries_on_rate_limit(self, mock_sleep, mock_get):
        """Test retry behavior on rate limit (429)."""
        mock_rate_limit = Mock()
        mock_rate_limit.status_code = 429
        mock_rate_limit.headers = {}

        mock_success = Mock()
        mock_success.status_code = 200
        mock_success.json.return_value = [{"id": 1}]

        mock_get.side_effect = [mock_rate_limit, mock_success]

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=Mock()
        )

        result = extractor.fetch_page_data(1, "2024-01-01T00:00:00Z")

        assert len(result) == 1
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    @patch("github_issue_extractor.requests.get")
    def test_returns_none_on_client_error(self, mock_get):
        """Test returning None on client error (401)."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        extractor = GitHubIssueExtractor(
            github_token="invalid_token", s3_bucket="test-bucket", s3_client=Mock()
        )

        result = extractor.fetch_page_data(1, "2024-01-01T00:00:00Z")

        assert result is None


class TestRunExtraction:
    """Tests for run_extraction method."""

    def test_returns_result_dict(self):
        """Test that run_extraction returns expected result structure."""
        mock_s3 = Mock()
        mock_s3.exceptions = Mock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey()

        extractor = GitHubIssueExtractor(
            github_token="test_token",
            s3_bucket="test-bucket",
            repo_owner="test-owner",
            repo_name="test-repo",
            s3_client=mock_s3,
        )

        # Mock fetch_page_data to return empty (end of data)
        with patch.object(extractor, "fetch_page_data", return_value=[]):
            result = extractor.run_extraction()

        assert "total_saved" in result
        assert "final_watermark" in result
        assert "repo" in result
        assert "s3_bucket" in result
        assert result["repo"] == "test-owner/test-repo"
        assert result["s3_bucket"] == "test-bucket"

    def test_processes_multiple_pages(self):
        """Test processing multiple pages of data."""
        mock_s3 = Mock()
        mock_s3.exceptions = Mock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey()

        extractor = GitHubIssueExtractor(
            github_token="test_token", s3_bucket="test-bucket", s3_client=mock_s3
        )

        # Simulate 2 batches of data then end
        page_data = [
            {"id": i, "updated_at": f"2024-01-0{i}T00:00:00Z"} for i in range(1, 4)
        ]
        call_count = [0]

        def mock_fetch(page_num, watermark):
            call_count[0] += 1
            if call_count[0] <= 3:
                return page_data
            return []

        with patch.object(extractor, "fetch_page_data", side_effect=mock_fetch):
            result = extractor.run_extraction()

        assert result["total_saved"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
