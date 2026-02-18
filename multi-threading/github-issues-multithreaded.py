import requests
import json
import os
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. Load environment variables
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# ================= CONFIGURATION =================
REPO_OWNER = "pandas-dev"
REPO_NAME = "pandas"
STATE_FILE = "issue_state.json"

# Output Logic: Batch files
today_str = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = "data"

DEFAULT_START_DATE = "2020-01-01T00:00:00Z"

# 🟢 Stepping Multi-threaded Config
STEP_SIZE = 5  # Concurrent pages per batch (step size)
MAX_WORKERS = 5  # Thread pool size (should match step size)
PAGE_SIZE = 100  # GitHub API items per page
BATCH_SIZE = (
    500  # Save to CSV every 500 records (affects storage frequency, not API requests)
)
batch_file_counter = 0  # Global file counter
# =================================================


def get_last_sync_time():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f).get("last_updated", DEFAULT_START_DATE)
        except:
            pass
    return DEFAULT_START_DATE


def save_last_sync_time(timestamp):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_updated": timestamp}, f, indent=4)
    print(f"💾 Checkpoint: Watermark updated to {timestamp}")


def flush_buffer_to_csv(data_buffer):
    """
    Write this batch of data to a new CSV file
    """
    global batch_file_counter

    if not data_buffer:
        return 0

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(data_buffer)

    # Filter PRs
    # if "pull_request" in df.columns:
    #     df_issues = df[df["pull_request"].isna()].copy()
    # else:
    #     df_issues = df.copy()
    df_issues = df.copy()  # Keep both Issues and PRs; filter later during analysis

    if df_issues.empty:
        # Return 0 to signal processing complete (even if only PRs, no issues)
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
    ]
    existing_cols = [c for c in cols_to_keep if c in df_issues.columns]

    # Generate filename: data/issues_2026-02-09_batch_001.csv
    batch_file_counter += 1
    output_csv = f"{OUTPUT_DIR}/issues_{today_str}_batch_{batch_file_counter:03d}.csv"

    df_issues[existing_cols].to_csv(
        output_csv, mode="w", header=True, index=False, encoding="utf-8-sig"
    )
    print(f"   💾 Saved {len(df_issues)} issues to {output_csv}")
    return len(df_issues)


def fetch_page_data(page_num, watermark):
    """
    Worker task.
    Return value design:
    - List [...]: Successfully fetched data
    - List []:    Success, but page is empty (end of data)
    - None:       ❌ Error occurred (network/timeout), mark as failed
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

    try:
        # Retry logic
        for _ in range(3):
            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # GitHub sometimes returns 404 when paginating past the end
                return []
            elif response.status_code == 429 or response.status_code == 403:
                print(f"⚠️ Page {page_num} hit rate limit. Sleeping 5s...")
                time.sleep(5)
            else:
                print(f"❌ Page {page_num} failed: {response.status_code}")
                return None  # Mark as Error
    except Exception as e:
        print(f"❌ Page {page_num} exception: {e}")

    return None  # Mark as Error


def run_stepping_extraction():
    watermark = get_last_sync_time()
    print(f"--- Starting Stepping Multi-threaded Extraction ---")
    print(f"Target: {REPO_OWNER}/{REPO_NAME}")
    print(f"Since:  {watermark}")
    print(f"Step Size: {STEP_SIZE} pages concurrent")

    current_start_page = 1
    total_saved_count = 0
    global_max_timestamp = watermark

    # Create thread pool
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    while True:
        # 1. Generate task list [1,2,3,4,5] -> [6,7,8,9,10] ...
        pages_to_fetch = list(range(current_start_page, current_start_page + STEP_SIZE))
        print(f"🚀 Launching batch: Pages {pages_to_fetch}...")

        batch_data = []
        batch_has_error = False  # Flag: did this batch have network errors?
        is_end_of_data = False  # Flag: did we reach the end of data?

        # 2. Submit tasks
        futures = {
            executor.submit(fetch_page_data, p, watermark): p for p in pages_to_fetch
        }

        for future in as_completed(futures):
            page_num = futures[future]
            data = future.result()

            if data is None:
                # ❌ Network error occurred
                batch_has_error = True
                print(f"⚠️ Batch corrupted: Page {page_num} failed.")
            else:
                # ✅ Successfully fetched data (may be empty list)
                if data:
                    batch_data.extend(data)
                    # If page has fewer than PAGE_SIZE items, it's the last page
                    if len(data) < PAGE_SIZE:
                        is_end_of_data = True
                else:
                    # Empty list means we've paginated past the end
                    is_end_of_data = True

        # 3. Save this batch's data (save whatever we got, even if errors occurred)
        if batch_data:
            # Write to CSV
            flush_buffer_to_csv(batch_data)
            total_saved_count += len(batch_data)

            # Find the max timestamp in this batch
            # Note: Calculate timestamp even if batch only has PRs, otherwise watermark won't advance
            df_temp = pd.DataFrame(batch_data)
            current_batch_max_ts = df_temp["updated_at"].max()

            # 4. 🔥 Core watermark safety logic 🔥
            if not batch_has_error:
                # Only update watermark when batch is completely error-free
                if current_batch_max_ts > global_max_timestamp:
                    global_max_timestamp = current_batch_max_ts
                    save_last_sync_time(global_max_timestamp)
            else:
                print(
                    f"🛑 Batch contained errors. Watermark NOT updated to ensure data integrity."
                )

        else:
            # If batch_data is empty and no errors, we've truly reached the end
            if not batch_has_error:
                print("🏁 Batch returned no data.")
                is_end_of_data = True

        # 5. Check if we should stop
        if is_end_of_data:
            print("🏁 Reached the end of pagination.")
            break

        # On error, we could break or continue to next batch (strategy choice)
        # Here we continue, since we've already saved whatever data we got

        # 6. Step forward
        current_start_page += STEP_SIZE

    executor.shutdown()
    print(f"--- Job Complete. Total saved today: {total_saved_count} ---")

    # Final watermark save (if needed)
    if global_max_timestamp > watermark:
        save_last_sync_time(global_max_timestamp)


if __name__ == "__main__":
    if not GITHUB_TOKEN:
        print("Error: GITHUB_TOKEN not found.")
    else:
        run_stepping_extraction()
