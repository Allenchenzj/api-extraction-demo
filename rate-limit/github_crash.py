import requests
import time


def github_kamikaze():
    print("🚀 Starting GitHub Search API stress test (limit: 10 requests/min)...")
    print("⚠️ Note: Without authentication, this limit applies to your IP.")

    url = "https://api.github.com/search/repositories?q=language:python"

    # Loop 15 times, guaranteed to exceed limit
    for i in range(1, 16):
        print(f"[{i}] Requesting...", end=" ")

        # No protection, direct request
        response = requests.get(url)

        print(response.text[:100] + "...")  # Print partial response to avoid flooding

        # Get remaining quota info for teaching demo
        remaining = response.headers.get("x-ratelimit-remaining", "?")

        if response.status_code == 200:
            print(f"✅ Success (remaining quota: {remaining})")

        elif response.status_code in [403, 429]:
            print(
                f"\n🔥 [BOOM] Rate limit triggered! Status code: {response.status_code}"
            )
            print(f"❌ Error message: {response.json().get('message')}")

            # Key point: GitHub tells you when the limit resets
            reset_time = response.headers.get("x-ratelimit-reset")
            print(f"⏳ Reset timestamp (Unix): {reset_time}")
            break
        else:
            print(f"❌ Other error: {response.status_code}")


if __name__ == "__main__":
    github_kamikaze()
