import requests
import concurrent.futures
import time

BASE_URL = "https://api.jikan.moe/v4"


def fetch_detail_unsafe(anime_id):
    """Request function without any protection"""
    url = f"{BASE_URL}/anime/{anime_id}/full"
    try:
        # No sleep here, no protection at all
        response = requests.get(url)

        if response.status_code == 200:
            print(f"✅ ID {anime_id}: Success")
            return "OK"
        elif response.status_code == 429:
            print(f"🔥 [BOOM] ID {anime_id}: Hit rate limit 429! (Demo successful)")
            return "429"
        else:
            print(f"❌ ID {anime_id}: Status code {response.status_code}")
            return "ERR"
    except Exception as e:
        print(f"💀 Network error: {e}")
        return "ERR"


def run_stress_test():
    print("🚀 Fetching top 50 anime list...")
    # Get the list first
    try:
        # Fetch extra page to get enough IDs
        r1 = requests.get(f"{BASE_URL}/top/anime")
        r2 = requests.get(f"{BASE_URL}/top/anime?page=2")
        data1 = r1.json().get("data", [])
        data2 = r2.json().get("data", [])

        # Extract IDs, collect 50 targets
        all_anime = (data1 + data2)[:50]
        id_list = [a["mal_id"] for a in all_anime]

        print(
            f"📋 Prepared {len(id_list)} targets. Starting 8-thread concurrent attack!\n"
        )

        # --- Core: Use thread pool to speed up ---
        # As long as thread count > 3, the instant QPS will theoretically exceed Jikan's limit
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(fetch_detail_unsafe, id_list))

        # Count results
        crash_count = results.count("429")
        print(
            f"\n📊 Test completed. {len(id_list)} requests total, rate limited {crash_count} times."
        )

        if crash_count > 0:
            print(
                "🎉 Congratulations! Demo successful, you successfully crashed the program!"
            )
        else:
            print("🤔 Still didn't crash? Jikan's server might have scaled up today...")

    except Exception as e:
        print(f"Failed to initialize list: {e}")


if __name__ == "__main__":
    run_stress_test()
