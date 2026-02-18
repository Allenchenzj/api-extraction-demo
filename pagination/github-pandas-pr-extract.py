import requests
import csv
import time
import os
from dotenv import load_dotenv

# ================= CONFIGURATION =================
# 1. Load environment variables
load_dotenv()

# 2. Get GitHub Token
# GitHub API rate limit: 60/hr (unauth) vs 5000/hr (auth). Always use a token!
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Target Repository (Owner/Name)
REPO_OWNER = "pandas-dev"
REPO_NAME = "pandas"

# Target settings
TARGET_COUNT = 5000
ITEMS_PER_PAGE = 10  # User requested 10 items per page
OUTPUT_FILE = "pandas_recent_prs.csv"
# =================================================

def fetch_pull_requests():
    """
    Fetches Pull Requests from GitHub API using pagination.
    """
    base_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls"
    all_prs = []
    current_page = 1
    
    # Header Authentication
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    print(f"🚀 Starting extraction from {REPO_OWNER}/{REPO_NAME}...")
    print(f"🎯 Target: {TARGET_COUNT} PRs (Batch size: {ITEMS_PER_PAGE})")

    while len(all_prs) < TARGET_COUNT:
        print(f"📡 Requesting page {current_page}...")
        
        try:
            # API Parameters
            params = {
                "state": "all",      # 'open', 'closed', or 'all'. 'all' gives the true history.
                "sort": "created",   # Sort by creation date
                "direction": "desc", # Newest first
                "per_page": ITEMS_PER_PAGE, # 10 items per page
                "page": current_page
            }
            
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ Request failed: Status {response.status_code} - {response.text}")
                break
            
            results = response.json()
            
            if not results:
                print("🏁 No more data available from API.")
                break
            
            # --- LOGGING DETAIL ---
            count_this_page = len(results)
            current_total = len(all_prs) + count_this_page
            print(f"   ✅ Page {current_page} success: Retrieved {count_this_page} PRs. (Total accumulated: {current_total})")
            # ----------------------

            all_prs.extend(results)
            current_page += 1
            
            # Stop if we hit the target count to avoid unnecessary requests
            if len(all_prs) >= TARGET_COUNT:
                break

            # Sleep to be polite
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ An error occurred: {e}")
            break

    # Return exactly the number requested (Slicing)
    return all_prs[:TARGET_COUNT]

def save_to_csv(prs, filename):
    """
    Saves selected PR fields to a CSV file.
    """
    if not prs:
        print("⚠️ No data to save.")
        return

    print(f"💾 Saving {len(prs)} records to {filename}...")

    # Define columns. GitHub JSON is nested, so we need to flatten it below.
    headers = [
        "id", "number", "title", "user", "state", 
        "created_at", "updated_at", "html_url"
    ]

    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for pr in prs:
                # TRANSFORM: Extract specific fields from the nested JSON
                # The 'user' field in JSON is a dictionary, we just want the login name
                user_login = pr.get("user", {}).get("login", "Unknown")
                
                writer.writerow({
                    "id": pr.get("id"),
                    "number": pr.get("number"),
                    "title": pr.get("title"),
                    "user": user_login, # Flattened data
                    "state": pr.get("state"),
                    "created_at": pr.get("created_at"),
                    "updated_at": pr.get("updated_at"),
                    "html_url": pr.get("html_url")
                })
        print("✅ Data saved successfully!")
        
    except IOError as e:
        print(f"❌ Failed to write to file: {e}")

if __name__ == "__main__":
    if not GITHUB_TOKEN:
        print("❌ ERROR: 'GITHUB_TOKEN' not found in .env file.")
    else:
        pr_data = fetch_pull_requests()
        save_to_csv(pr_data, OUTPUT_FILE)