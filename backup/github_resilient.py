import requests
import logging
import time
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("GitHubBot")


class GitHubResilientExtractor:
    def __init__(self):
        self.session = requests.Session()
        # Even without auth, setting a User-Agent is good practice
        self.session.headers.update({"User-Agent": "MyClassDemo/1.0"})

    # --- Core Solution ---
    # Strategy: Exponential backoff when hitting rate limit
    # GitHub Search has strict limits, may need to wait 1 min, so set max wait time longer (60s)
    @retry(
        stop=stop_after_attempt(6),  # Max 6 retries
        wait=wait_exponential(multiplier=1, min=2, max=60),  # Max wait 60 seconds
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def search_repo(self, query):
        url = f"https://api.github.com/search/repositories?q={query}"

        response = self.session.get(url)

        # [Key Point] GitHub rate limit sometimes returns 403, sometimes 429
        # And 403 could be permission denied, so check Header or Body
        if response.status_code in [403, 429]:
            # Check if 403 is actually caused by rate limiting
            remaining = response.headers.get("x-ratelimit-remaining")
            if remaining == "0":
                reset_time = response.headers.get("x-ratelimit-reset")
                logger.warning(
                    f"🛑 Rate limit triggered! Quota is 0. Reset timestamp: {reset_time}"
                )
                # Raise exception to trigger retry
                raise Exception("GitHub Rate Limit Hit")

        response.raise_for_status()
        return response.json()

    def run(self):
        # Intentionally run 12 times, will exceed limit (quota is 10)
        targets = [f"demo_{i}" for i in range(12)]

        logger.info(
            f"🚀 Starting {len(targets)} requests (expected to pause after 10th)..."
        )

        for i, t in enumerate(targets):
            try:
                # Search different keywords each time to avoid local caching
                data = self.search_repo(f"python_library_{i}")
                total_count = data.get("total_count", 0)

                # Print remaining quota for demo effect
                remaining = self.session.get("https://api.github.com/zen").headers.get(
                    "x-ratelimit-remaining", "?"
                )  # Simple header fetch trick

                logger.info(f"✅ [{i+1}/12] Success (result count: {total_count})")

            except Exception as e:
                logger.error(f"💀 Complete failure: {e}")


if __name__ == "__main__":
    extractor = GitHubResilientExtractor()
    extractor.run()
