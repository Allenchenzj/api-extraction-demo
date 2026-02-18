import requests
import csv
import time
import os
from dotenv import load_dotenv

# ================= CONFIGURATION =================
# 1. Load environment variables
load_dotenv()

# 2. Get Access Token safely
ACCESS_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")

TARGET_COUNT = 100
OUTPUT_FILE = "top_100_popular_movies.csv"
# =================================================

def fetch_popular_movies():
    """
    Fetches movies using TMDB API Read Access Token (Bearer Auth).
    """
    base_url = "https://api.themoviedb.org/3/movie/popular"
    all_movies = []
    current_page = 1
    
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "accept": "application/json"
    }

    print(f"🚀 Starting extraction. Target: {TARGET_COUNT} movies...")

    while len(all_movies) < TARGET_COUNT:
        print(f"📡 Requesting page {current_page}...")
        
        try:
            params = {
                "language": "en-US", 
                "page": current_page
            }
            
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ Request failed: Status {response.status_code} - {response.text}")
                break
            
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                print("🏁 No more data available from API.")
                break
            
            # --- LOGGING DETAIL ADDED HERE ---
            count_this_page = len(results)
            current_total = len(all_movies) + count_this_page
            print(f"   ✅ Page {current_page} success: Retrieved {count_this_page} movies. (Total accumulated: {current_total})")
            # ---------------------------------

            all_movies.extend(results)
            current_page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ An error occurred: {e}")
            break

    return all_movies[:TARGET_COUNT]

def save_to_csv(movies, filename):
    """
    Saves the list of movie dictionaries to a CSV file.
    """
    if not movies:
        print("⚠️ No data to save.")
        return

    print(f"💾 Saving {len(movies)} records to {filename}...")

    headers = [
        "id", "title", "original_title", "release_date", 
        "vote_average", "vote_count", "popularity", "overview"
    ]

    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for movie in movies:
                clean_overview = movie.get("overview", "").replace("\n", " ")
                writer.writerow({
                    "id": movie.get("id"),
                    "title": movie.get("title"),
                    "original_title": movie.get("original_title"),
                    "release_date": movie.get("release_date"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                    "popularity": movie.get("popularity"),
                    "overview": clean_overview
                })
        print("✅ Data saved successfully!")
        
    except IOError as e:
        print(f"❌ Failed to write to file: {e}")

if __name__ == "__main__":
    if not ACCESS_TOKEN:
        print("❌ ERROR: 'TMDB_READ_ACCESS_TOKEN' not found in .env file.")
    else:
        movies_data = fetch_popular_movies()
        save_to_csv(movies_data, OUTPUT_FILE)