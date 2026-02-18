import logging
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Configure logging: include thread name to see which thread is retrying
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("JikanUltimate")


class UltimateExtractor:
    def __init__(self):
        # Each thread having its own session might be better, but requests session is thread-safe
        self.session = requests.Session()
        self.base_url = "https://api.jikan.moe/v4"

    # --- Core: Give each concurrent thread a "bulletproof vest" ---
    @retry(
        stop=stop_after_attempt(10),  # Give enough retry attempts
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def fetch_detail(self, anime_id):
        # Print which thread is currently running
        thread_name = threading.current_thread().name

        # Intentionally removed time.sleep to let threads hit the server at full speed
        url = f"{self.base_url}/anime/{anime_id}/full"
        response = self.session.get(url, timeout=5)

        if response.status_code == 429:
            # Raise exception to trigger tenacity
            # Note: We don't print Error here, we throw to let tenacity print Warning
            raise Exception(f"Rate limited 429")

        if response.status_code >= 500:
            raise Exception(f"Server error {response.status_code}")

        return response.json()["data"]["title"]

    def run_concurrent(self):
        # Same batch of IDs, but this time we send them all at once
        target_ids = [
            57555,
            9253,
            59978,
            5114,
            52991,
            9969,
            34096,
            820,
            11061,
            41467,
        ] * 2  # Duplicate to 20, more pressure

        logger.info(
            f"🚀 Starting ultimate mode: 10 threads concurrently fetching {len(target_ids)} tasks..."
        )

        with ThreadPoolExecutor(max_workers=10) as executor:
            # Distribute tasks to thread pool
            futures = {
                executor.submit(self.fetch_detail, mid): mid for mid in target_ids
            }

            for future in futures:
                mid = futures[future]
                try:
                    # result() blocks until task completes (including successful retry)
                    title = future.result()
                    logger.info(f"✅ Final success ID {mid}: {title}")
                except Exception as e:
                    logger.error(f"💀 Complete failure ID {mid}: {e}")


if __name__ == "__main__":
    extractor = UltimateExtractor()
    extractor.run_concurrent()
