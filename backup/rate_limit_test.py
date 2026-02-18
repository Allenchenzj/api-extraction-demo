import os
import requests
from dotenv import load_dotenv

load_dotenv()


class TMDBStressTester:
    def __init__(self):
        self.base_url = "https://api.themoviedb.org/3"
        self.token = os.getenv("TMDB_READ_ACCESS_TOKEN")
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        )

    def run_until_death(self):
        page = 1
        print(f"🚀 Starting stress test, flooding {self.base_url}/movie/popular ...")

        try:
            while True:
                # Note: Intentionally no time.sleep, pushing server limits
                response = self.session.get(
                    f"{self.base_url}/movie/popular", params={"page": page}
                )

                # Real-time status monitoring
                status = response.status_code

                if status == 200:
                    # Print progress, but not full JSON to avoid flooding
                    print(f"Page {page:04d}: [OK 200]", end="\r")
                    page += 1
                elif status == 429:
                    print(f"\n\n🛑 Hit the wall! Rate Limit triggered (status: 429)")
                    # Key: Try to get server-requested wait time
                    retry_after = response.headers.get("Retry-After", "unknown")
                    print(f"Server requested wait (Retry-After): {retry_after} seconds")
                    print("Headers detail:", response.headers)
                    break
                else:
                    print(f"\n\n⚠️ Program terminated abnormally, status: {status}")
                    print(response.text)
                    break

                # To prevent permanent IP ban from TMDB, recommend stopping at 500 pages,
                # or let it run if you really want to see 429.
                if page > 1000:
                    print(
                        "\n\n🏁 Reached 1000 page safety limit, stopping test to prevent IP ban."
                    )
                    break

        except KeyboardInterrupt:
            print("\n\n👋 User manually stopped the test.")


if __name__ == "__main__":
    tester = TMDBStressTester()
    tester.run_until_death()
