import os
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class TMDBPressureClient:
    def __init__(self):
        self.token = os.getenv("TMDB_READ_ACCESS_TOKEN")
        self.session = requests.Session()
        # Set up session headers
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "User-Agent": "PressureTester/1.0",
            }
        )

    def fetch_unique_detail(self, movie_id):
        """Fetch different movie details to bypass cache and trigger rate limiting"""
        url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        try:
            # Set short timeout to prevent thread from hanging
            res = self.session.get(url, timeout=5)

            if res.status_code == 200:
                # Don't print on success to keep console clean, only show progress at the end
                return True
            elif res.status_code == 429:
                print(f"\n🔥 [HIT!] 429 Too Many Requests at Movie ID: {movie_id}")
                # Print rate limit related headers to see server response
                print(f"Retry-After: {res.headers.get('Retry-After')}")
                return False
            else:
                print(f"\n⚠️ Unexpected Status {res.status_code} at ID {movie_id}")
                return False
        except Exception as e:
            print(f"\n❌ Request Error: {e}")
            return False


def run_test_v2():
    client = TMDBPressureClient()

    # Step 1: Get 20 real existing movie IDs
    print("📡 Fetching real movie IDs...")
    pop_res = client.session.get("https://api.themoviedb.org/3/movie/popular")
    valid_ids = [m["id"] for m in pop_res.json()["results"]]

    # Step 2: Duplicate these 20 IDs 100 times to create 2000 high-frequency requests
    test_ids = valid_ids * 100
    random.shuffle(test_ids)  # Shuffle the order

    print(f"🚀 Starting precision stress test: 50 Workers, {len(test_ids)} Requests...")

    with ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(client.fetch_unique_detail, test_ids))

    success_count = sum(1 for r in results if r)
    print(
        f"\nTest completed: {success_count} successful, {len(results) - success_count} failed/rate-limited."
    )


if __name__ == "__main__":
    run_test_v2()
