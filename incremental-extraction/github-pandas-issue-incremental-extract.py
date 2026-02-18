import requests
import json
import os
import pandas as pd
from datetime import datetime  # 1. Import datetime module
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# ================= CONFIGURATION =================
REPO_OWNER = "pandas-dev"
REPO_NAME = "pandas"
STATE_FILE = "issue_state.json"

# 2. Dynamically generate filename: e.g., "issues_2026-02-08.csv"
# Each script run checks today's date
today_str = datetime.now().strftime("%Y-%m-%d")
OUTPUT_CSV = f"issues_{today_str}.csv"

DEFAULT_START_DATE = "2025-12-01T00:00:00Z"
# =================================================


def get_last_sync_time():
    """Reads the last updated timestamp from local JSON file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f).get("last_updated", DEFAULT_START_DATE)
        except:
            pass
    return DEFAULT_START_DATE


def save_last_sync_time(timestamp):
    """Saves the new watermark to local JSON file."""
    with open(STATE_FILE, "w") as f:
        json.dump({"last_updated": timestamp}, f, indent=4)
    print(f"💾 Success: Watermark state updated to {timestamp}")


def fetch_and_save_incremental_issues():
    watermark = get_last_sync_time()
    print(f"--- Starting Incremental Extraction ---")
    print(f"Target: {REPO_OWNER}/{REPO_NAME}")
    print(f"Output File: {OUTPUT_CSV}")  # Print current output filename
    print(f"Since:  {watermark}")

    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/issues"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    current_page = 1
    total_saved_count = 0
    global_max_timestamp = watermark

    while True:
        print(f"📡 Fetching page {current_page}...", end=" ")

        params = {
            "state": "all",
            "since": watermark,
            "sort": "updated",
            "direction": "asc",
            "per_page": 100,
            "page": current_page,
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code != 200:
                print(f"\n❌ Error: Status {response.status_code}")
                break

            data = response.json()
            if not data:
                print("\n🏁 No more data available.")
                break

            # --- Convert to Pandas ---
            df = pd.DataFrame(data)

            # --- Filter out PRs ---
            if "pull_request" in df.columns:
                df_issues = df[df["pull_request"].isna()].copy()
            else:
                df_issues = df.copy()

            # --- Save to CSV (Daily Partition) ---
            if not df_issues.empty:
                # Check if TODAY'S file exists
                file_exists = os.path.isfile(OUTPUT_CSV)

                # Data Cleaning
                if "user" in df_issues.columns:
                    df_issues["user"] = df_issues["user"].apply(
                        lambda x: x.get("login") if isinstance(x, dict) else None
                    )

                # Select columns
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

                # Append to CSV
                df_issues[existing_cols].to_csv(
                    OUTPUT_CSV,
                    mode="a",  # Append mode
                    header=not file_exists,  # Only write header if this is the first run TODAY
                    index=False,
                    encoding="utf-8-sig",
                )

                count = len(df_issues)
                total_saved_count += count
                print(f"✅ Appended {count} issues to {OUTPUT_CSV}")

            # --- Update Memory Watermark ---
            batch_max_ts = df["updated_at"].max()
            if batch_max_ts > global_max_timestamp:
                global_max_timestamp = batch_max_ts

            if len(data) < 100:
                print("🏁 Last page reached.")
                break

            current_page += 1

        except Exception as e:
            print(f"\n❌ Critical Error: {e}")
            break

    # --- Final State Update ---
    print(f"--- Job Complete. Total saved today: {total_saved_count} ---")

    if global_max_timestamp > watermark:
        save_last_sync_time(global_max_timestamp)
    else:
        print("💤 No new updates found. Watermark unchanged.")


if __name__ == "__main__":
    if not GITHUB_TOKEN:
        print("Error: GITHUB_TOKEN not found.")
    else:
        fetch_and_save_incremental_issues()
