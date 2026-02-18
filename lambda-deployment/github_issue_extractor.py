"""
GitHub Issue Extractor Module

A class-based implementation for extracting GitHub issues from a repository
and storing them in S3 with incremental sync support.
"""

import json
import os
import time
import random
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any

import requests
import pandas as pd
import boto3


class GitHubIssueExtractor:
    """
    Extracts GitHub issues from a repository using the GitHub API
    and stores them incrementally in S3.

    Attributes:
        github_token: GitHub Personal Access Token
        s3_bucket: S3 bucket name for data storage
        repo_owner: GitHub repository owner
        repo_name: GitHub repository name
        s3_client: Boto3 S3 client
    """

    DEFAULT_START_DATE = "2024-01-01T00:00:00Z"

    # Extraction configuration
    STEP_SIZE = 3  # Concurrent pages per batch
    MAX_WORKERS = 3  # Thread pool size (optimized for Lambda)
    PAGE_SIZE = 100  # GitHub API max per page
    MAX_RETRIES = 5  # Max retries for backoff
    BASE_DELAY = 1  # Initial backoff delay (seconds)
    MAX_DELAY = 32  # Max backoff delay (seconds)

    def __init__(
        self,
        github_token: str,
        s3_bucket: str,
        repo_owner: str = "pandas-dev",
        repo_name: str = "pandas",
        s3_client: Optional[Any] = None,
    ):
        """
        Initialize the GitHubIssueExtractor.

        Args:
            github_token: GitHub Personal Access Token for API authentication
            s3_bucket: S3 bucket name for storing extracted data and state
            repo_owner: GitHub repository owner (default: pandas-dev)
            repo_name: GitHub repository name (default: pandas)
            s3_client: Optional boto3 S3 client (for testing/mocking)
        """
        if not github_token:
            raise ValueError("github_token is required")
        if not s3_bucket:
            raise ValueError("s3_bucket is required")

        self.github_token = github_token
        self.s3_bucket = s3_bucket
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.s3_client = s3_client or boto3.client("s3")

        # State tracking
        self._batch_file_counter = 0
        self._today_str = datetime.now().strftime("%Y-%m-%d")

        # S3 paths
        self.state_file_key = "issue_state.json"
        self.output_prefix = "data"

    def get_last_sync_time(self) -> str:
        """
        Read last sync timestamp from S3.

        Returns:
            ISO format timestamp string of last sync, or DEFAULT_START_DATE
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket, Key=self.state_file_key
            )
            state_data = json.loads(response["Body"].read().decode("utf-8"))
            return state_data.get("last_updated", self.DEFAULT_START_DATE)
        except self.s3_client.exceptions.NoSuchKey:
            print(f"No state file found in S3, starting from {self.DEFAULT_START_DATE}")
            return self.DEFAULT_START_DATE
        except Exception as e:
            print(f"Error reading state from S3: {e}")
            return self.DEFAULT_START_DATE

    def save_last_sync_time(self, timestamp: str) -> bool:
        """
        Write last sync timestamp to S3.

        Args:
            timestamp: ISO format timestamp string

        Returns:
            True if successful, False otherwise
        """
        try:
            state_data = json.dumps({"last_updated": timestamp}, indent=4)
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.state_file_key,
                Body=state_data.encode("utf-8"),
                ContentType="application/json",
            )
            print(f"Checkpoint: Watermark updated to {timestamp}")
            return True
        except Exception as e:
            print(f"Error saving state to S3: {e}")
            return False

    def flush_buffer_to_csv(self, data_buffer: List[Dict]) -> int:
        """
        Write batch of data to a new CSV file in S3.

        Args:
            data_buffer: List of issue dictionaries from GitHub API

        Returns:
            Number of issues saved, or 0 on failure
        """
        if not data_buffer:
            return 0

        df = pd.DataFrame(data_buffer)
        df_issues = df.copy()

        if df_issues.empty:
            return 0

        # Data cleaning - extract user login
        if "user" in df_issues.columns:
            df_issues["user"] = df_issues["user"].apply(
                lambda x: x.get("login") if isinstance(x, dict) else None
            )

        cols_to_keep = [
            "id",
            "number",
            "title",
            "user",
            "state",
            "created_at",
            "updated_at",
            "body",
            "html_url",
        ]
        existing_cols = [c for c in cols_to_keep if c in df_issues.columns]

        # Generate S3 key
        self._batch_file_counter += 1
        s3_key = (
            f"{self.output_prefix}/issues_{self._today_str}"
            f"_batch_{self._batch_file_counter:03d}.csv"
        )

        # Write to in-memory buffer then upload to S3
        csv_buffer = io.StringIO()
        df_issues[existing_cols].to_csv(
            csv_buffer, mode="w", header=True, index=False, encoding="utf-8"
        )

        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=csv_buffer.getvalue().encode("utf-8"),
                ContentType="text/csv",
            )
            print(f"Saved {len(df_issues)} issues to s3://{self.s3_bucket}/{s3_key}")
            return len(df_issues)
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return 0

    def fetch_page_data(self, page_num: int, watermark: str) -> Optional[List[Dict]]:
        """
        Fetch a single page of issues from GitHub API with retry logic.

        Args:
            page_num: Page number to fetch (1-indexed)
            watermark: ISO timestamp to filter issues updated since

        Returns:
            List of issue dictionaries, empty list if end of data,
            or None on unrecoverable error
        """
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/issues"
        headers = {
            "Authorization": f"Bearer {self.github_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        params = {
            "state": "all",
            "since": watermark,
            "sort": "updated",
            "direction": "asc",
            "per_page": self.PAGE_SIZE,
            "page": page_num,
        }

        current_delay = self.BASE_DELAY

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=15)

                # Success
                if response.status_code == 200:
                    return response.json()

                # End of data (404 or 422)
                if response.status_code in [404, 422]:
                    print(
                        f"Page {page_num} reached end (Status {response.status_code})"
                    )
                    return []

                # Rate limit (429 or 403)
                if response.status_code in [429, 403]:
                    sleep_time = current_delay + random.uniform(0, 1)
                    if "Retry-After" in response.headers:
                        sleep_time = float(response.headers["Retry-After"]) + 1
                    print(
                        f"Page {page_num} rate limited. "
                        f"Waiting {sleep_time:.2f}s (Attempt {attempt})"
                    )
                    time.sleep(sleep_time)
                    current_delay = min(current_delay * 2, self.MAX_DELAY)
                    continue

                # Server error (5xx) - retry
                if response.status_code >= 500:
                    print(
                        f"Page {page_num} server error {response.status_code}. Retrying..."
                    )
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.MAX_DELAY)
                    continue

                # Client error (400, 401) - fatal
                print(f"Page {page_num} fatal error: {response.status_code}")
                return None

            except Exception as e:
                print(f"Page {page_num} exception: {e}. Retrying...")
                time.sleep(current_delay)
                current_delay = min(current_delay * 2, self.MAX_DELAY)

        print(f"Page {page_num} failed after {self.MAX_RETRIES} attempts")
        return None

    def run_extraction(self) -> Dict[str, Any]:
        """
        Execute the incremental extraction process.

        Fetches issues updated since last sync, saves to S3 in batches,
        and updates the watermark checkpoint.

        Returns:
            Dictionary with extraction results including total_saved,
            final_watermark, repo, and s3_bucket
        """
        watermark = self.get_last_sync_time()
        print("--- Starting GitHub Issue Extraction ---")
        print(f"Target: {self.repo_owner}/{self.repo_name}")
        print(f"Since: {watermark}")
        print(f"Config: Step={self.STEP_SIZE}, Workers={self.MAX_WORKERS}")

        current_start_page = 1
        total_saved_count = 0
        global_max_timestamp = watermark

        # Reset batch counter for this run
        self._batch_file_counter = 0
        self._today_str = datetime.now().strftime("%Y-%m-%d")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            while True:
                # Generate task list
                pages_to_fetch = list(
                    range(current_start_page, current_start_page + self.STEP_SIZE)
                )
                print(f"Launching batch: Pages {pages_to_fetch}...")

                batch_data = []
                batch_has_error = False
                is_end_of_data = False

                # Submit tasks
                futures = {
                    executor.submit(self.fetch_page_data, p, watermark): p
                    for p in pages_to_fetch
                }

                for future in as_completed(futures):
                    page_num = futures[future]
                    data = future.result()

                    if data is None:
                        batch_has_error = True
                        print(f"Batch corrupted: Page {page_num} failed")
                    elif data:
                        batch_data.extend(data)
                        if len(data) < self.PAGE_SIZE:
                            is_end_of_data = True
                    else:
                        is_end_of_data = True

                # Process batch
                if batch_data:
                    self.flush_buffer_to_csv(batch_data)
                    total_saved_count += len(batch_data)

                    df_temp = pd.DataFrame(batch_data)
                    current_batch_max_ts = None
                    if "updated_at" in df_temp.columns:
                        current_batch_max_ts = df_temp["updated_at"].max()

                    # Atomic watermark update
                    if not batch_has_error:
                        if (
                            current_batch_max_ts
                            and current_batch_max_ts > global_max_timestamp
                        ):
                            global_max_timestamp = current_batch_max_ts
                            self.save_last_sync_time(global_max_timestamp)
                    else:
                        print("Batch contained errors. Watermark NOT updated.")
                else:
                    if not batch_has_error:
                        print("Batch returned no data.")
                        is_end_of_data = True

                if is_end_of_data:
                    print("Reached the end of pagination.")
                    break

                current_start_page += self.STEP_SIZE

        print(f"--- Job Complete. Total saved: {total_saved_count} ---")

        # Final watermark check
        if global_max_timestamp > watermark:
            self.save_last_sync_time(global_max_timestamp)

        return {
            "total_saved": total_saved_count,
            "final_watermark": global_max_timestamp,
            "repo": f"{self.repo_owner}/{self.repo_name}",
            "s3_bucket": self.s3_bucket,
        }
