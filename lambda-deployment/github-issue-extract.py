import requests
import json
import os
import pandas as pd
import time
import random  # 🟢 Required for Jitter
import io
import boto3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIGURATION =================
# Environment variables (set via Lambda configuration)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
S3_BUCKET = os.getenv("S3_BUCKET", "github-api-extraction-bucket")
REPO_OWNER = os.getenv("REPO_OWNER", "pandas-dev")
REPO_NAME = os.getenv("REPO_NAME", "pandas")

# S3 paths
STATE_FILE_KEY = "issue_state.json"
OUTPUT_PREFIX = "data"

# Output Logic: Batch files
today_str = datetime.now().strftime("%Y-%m-%d")

DEFAULT_START_DATE = "2024-01-01T00:00:00Z"

# 🟢 Stepping Multi-threaded Config (reduced for Lambda)
STEP_SIZE = 3  # Concurrent pages per batch
MAX_WORKERS = 3  # Thread pool size (reduced for Lambda CPU)
PAGE_SIZE = 100  # GitHub API Max
MAX_RETRIES = 5  # Max retries for backoff
BASE_DELAY = 1  # Initial backoff delay (seconds)
MAX_DELAY = 32  # Max backoff delay
batch_file_counter = 0

# Initialize S3 client
s3_client = boto3.client("s3")
# =================================================


def get_last_sync_time():
    """Read last sync timestamp from S3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=STATE_FILE_KEY)
        state_data = json.loads(response["Body"].read().decode("utf-8"))
        return state_data.get("last_updated", DEFAULT_START_DATE)
    except s3_client.exceptions.NoSuchKey:
        print(f"📋 No state file found in S3, starting from {DEFAULT_START_DATE}")
        return DEFAULT_START_DATE
    except Exception as e:
        print(f"⚠️ Error reading state from S3: {e}")
        return DEFAULT_START_DATE


def save_last_sync_time(timestamp):
    """Write last sync timestamp to S3."""
    try:
        state_data = json.dumps({"last_updated": timestamp}, indent=4)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=STATE_FILE_KEY,
            Body=state_data.encode("utf-8"),
            ContentType="application/json",
        )
        print(f"💾 Checkpoint: Watermark updated to {timestamp} (S3)")
    except Exception as e:
        print(f"❌ Error saving state to S3: {e}")


def flush_buffer_to_csv(data_buffer):
    """
    Write this batch of data to a new CSV file in S3.
    """
    global batch_file_counter

    if not data_buffer:
        return 0

    df = pd.DataFrame(data_buffer)

    # Note: We keep PRs mixed with Issues. Filter them downstream if needed.
    df_issues = df.copy()

    if df_issues.empty:
        return 0

    # Data Cleaning
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
    batch_file_counter += 1
    s3_key = f"{OUTPUT_PREFIX}/issues_{today_str}_batch_{batch_file_counter:03d}.csv"

    # Write to in-memory buffer then upload to S3
    csv_buffer = io.StringIO()
    df_issues[existing_cols].to_csv(
        csv_buffer, mode="w", header=True, index=False, encoding="utf-8"
    )

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        print(f"   💾 Saved {len(df_issues)} issues to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"   ❌ Error uploading to S3: {e}")
        return 0

    return len(df_issues)


def fetch_page_data(page_num, watermark):
    """
    Worker task with ROBUST exponential backoff and 422 handling.
    """
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/issues"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    params = {
        "state": "all",
        "since": watermark,
        "sort": "updated",
        "direction": "asc",
        "per_page": PAGE_SIZE,
        "page": page_num,
    }

    current_delay = BASE_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)

            # ✅ Case 1: Success
            if response.status_code == 200:
                return response.json()

            # ✅ Case 2: End of Data (404 or 422)
            # 422 means "Pagination limit reached" or "Out of bounds"
            elif response.status_code in [404, 422]:
                print(
                    f"🏁 Page {page_num} reached end (Status {response.status_code})."
                )
                return []

            # 🛑 Case 3: Rate Limit (429 or 403)
            elif response.status_code in [429, 403]:
                # Calculate sleep time with Jitter
                sleep_time = current_delay + random.uniform(0, 1)

                # Check for strict Retry-After header
                if "Retry-After" in response.headers:
                    sleep_time = float(response.headers["Retry-After"]) + 1

                print(
                    f"⚠️ Page {page_num} Hit Rate Limit. Waiting {sleep_time:.2f}s... (Attempt {attempt})"
                )
                time.sleep(sleep_time)

                # Exponential Backoff
                current_delay = min(current_delay * 2, MAX_DELAY)

            # ❌ Case 4: Server Error (5xx) - Retry
            elif response.status_code >= 500:
                print(
                    f"❌ Page {page_num} Server Error {response.status_code}. Retrying..."
                )
                time.sleep(current_delay)
                current_delay = min(current_delay * 2, MAX_DELAY)

            # ☠️ Case 5: Client Error (400, 401) - Fatal
            else:
                print(f"❌ Page {page_num} Fatal Error: {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ Page {page_num} Exception: {e}. Retrying...")
            time.sleep(current_delay)
            current_delay = min(current_delay * 2, MAX_DELAY)

    # Failure after retries
    print(f"💀 Page {page_num} failed after {MAX_RETRIES} attempts.")
    return None


def run_stepping_extraction():
    watermark = get_last_sync_time()
    print(f"--- Starting Stepping Multi-threaded Extraction ---")
    print(f"Target: {REPO_OWNER}/{REPO_NAME}")
    print(f"Since:  {watermark}")
    print(f"Config: Step={STEP_SIZE}, Threads={MAX_WORKERS}")

    current_start_page = 1
    total_saved_count = 0
    global_max_timestamp = watermark

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    while True:
        # 1. Generate Task List
        pages_to_fetch = list(range(current_start_page, current_start_page + STEP_SIZE))
        print(f"🚀 Launching batch: Pages {pages_to_fetch}...")

        batch_data = []
        batch_has_error = False
        is_end_of_data = False

        # 2. Submit Tasks
        futures = {
            executor.submit(fetch_page_data, p, watermark): p for p in pages_to_fetch
        }

        for future in as_completed(futures):
            page_num = futures[future]
            data = future.result()

            if data is None:
                # ❌ Network Error / Timeout
                batch_has_error = True
                print(f"⚠️ Batch corrupted: Page {page_num} failed.")
            else:
                # ✅ Success (List could be empty)
                if data:
                    batch_data.extend(data)
                    # If page is not full, it's the last page
                    if len(data) < PAGE_SIZE:
                        is_end_of_data = True
                else:
                    # Empty list = End of pagination
                    is_end_of_data = True

        # 3. Process Batch
        if batch_data:
            # Save CSV (Best Effort)
            flush_buffer_to_csv(batch_data)
            total_saved_count += len(batch_data)

            # Calculate Timestamp
            df_temp = pd.DataFrame(batch_data)
            current_batch_max_ts = None
            if "updated_at" in df_temp.columns:
                current_batch_max_ts = df_temp["updated_at"].max()

            # 4. 🔥 Atomic Watermark Update 🔥
            if not batch_has_error:
                if current_batch_max_ts and current_batch_max_ts > global_max_timestamp:
                    global_max_timestamp = current_batch_max_ts
                    save_last_sync_time(global_max_timestamp)
            else:
                print(f"🛑 Batch contained errors. Watermark NOT updated.")

        else:
            if not batch_has_error:
                print("🏁 Batch returned no data.")
                is_end_of_data = True

        # 5. Check Termination
        if is_end_of_data:
            print("🏁 Reached the end of pagination.")
            break

        # 6. Step Forward
        current_start_page += STEP_SIZE

    executor.shutdown()
    print(f"--- Job Complete. Total saved today: {total_saved_count} ---")

    # Final watermark check
    if global_max_timestamp > watermark:
        save_last_sync_time(global_max_timestamp)

    return {
        "total_saved": total_saved_count,
        "final_watermark": global_max_timestamp,
        "repo": f"{REPO_OWNER}/{REPO_NAME}",
        "s3_bucket": S3_BUCKET,
    }


def lambda_handler(event, context):
    """
    AWS Lambda entry point.

    Args:
        event: Lambda event data (can contain override config)
        context: Lambda context object

    Returns:
        dict: Execution results
    """
    global REPO_OWNER, REPO_NAME, S3_BUCKET

    # Allow event-based overrides
    if event:
        if "repo_owner" in event:
            REPO_OWNER = event["repo_owner"]
        if "repo_name" in event:
            REPO_NAME = event["repo_name"]
        if "s3_bucket" in event:
            S3_BUCKET = event["s3_bucket"]

    if not GITHUB_TOKEN:
        return {
            "statusCode": 500,
            "body": "Error: GITHUB_TOKEN environment variable not set.",
        }

    try:
        result = run_stepping_extraction()
        return {"statusCode": 200, "body": result}
    except Exception as e:
        print(f"❌ Lambda execution failed: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}


if __name__ == "__main__":
    # Local testing
    if not GITHUB_TOKEN:
        print("Error: GITHUB_TOKEN not found.")
    else:
        result = run_stepping_extraction()
        print(f"Result: {result}")
