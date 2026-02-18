import requests
import time


class RedditCursorFetcher:
    def __init__(self):
        # Reddit public JSON endpoint
        self.base_url = "https://www.reddit.com/r/Java/new.json"
        self.session = requests.Session()
        # ⚠️ Key: Reddit is strict; set a unique User-Agent or you'll get 429
        self.session.headers.update(
            {"User-Agent": "MyDataEngineeringCourse/1.0 (Student Demo)"}
        )

    def fetch_stream(self, total_target=50, page_size=10):
        """
        :param total_target: Total number of items to fetch
        :param page_size: Custom page size (maps to API 'limit')
        """
        print(
            f"🚀 Starting cursor pagination demo (target: {total_target} items, {page_size} per page)..."
        )

        fetched_count = 0
        # 1. Initialize cursor: first page uses an empty cursor
        after_cursor = None

        page_num = 1

        while fetched_count < total_target:
            # 2. Build request params
            params = {
                "limit": page_size,  # user config: how many to fetch per page
                "after": after_cursor,  # key: send back the previous page token
            }

            try:
                print(
                    f"\n📡 Requesting page {page_num} (Cursor: {after_cursor if after_cursor else 'NULL'})..."
                )
                response = self.session.get(self.base_url, params=params)

                if response.status_code != 200:
                    print(f"❌ Request failed: {response.status_code}")
                    break

                data = response.json()
                posts = data["data"]["children"]

                if not posts:
                    print("🏁 No more data.")
                    break

                # 3. Process data
                for post in posts:
                    title = post["data"]["title"][:100] + "..."
                    print(f"   📝 {title}")

                fetched_count += len(posts)

                # 4. Key step: extract the next cursor
                # Reddit response clearly provides the next page token: 't3_xyz...'
                after_cursor = data["data"]["after"]

                if not after_cursor:
                    print("🏁 Server indicates no next page (after=None).")
                    break

                page_num += 1
                # Polite fetching
                time.sleep(1)

            except Exception as e:
                print(f"❌ Error: {e}")
                break

        print(f"\n✅ Demo complete! Fetched {fetched_count} items total.")


if __name__ == "__main__":
    fetcher = RedditCursorFetcher()

    # Scenario A: small sample, 5 per page, observe cursor changes
    fetcher.fetch_stream(total_target=15, page_size=5)

    # Scenario B: (optional) for throughput demo, set page_size to 100
    # fetcher.fetch_stream(total_target=100, page_size=100)
