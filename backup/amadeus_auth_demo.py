import requests
import os
import time
from dotenv import load_dotenv

load_dotenv()


class AmadeusFlightFetcher:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        # Amadeus test environment URL
        self.auth_url = "https://test.api.amadeus.com/v1/security/oauth2/token"
        self.base_url = "https://test.api.amadeus.com/v1"
        self.session = requests.Session()

    def get_token(self):
        """
        [OAuth2 Client Credentials Flow]
        Machine-to-machine auth: No user login needed, exchange Key+Secret for Token
        """
        print("🤖 Requesting machine token from Amadeus...")

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.api_key,
            "client_secret": self.api_secret,
        }

        try:
            response = requests.post(self.auth_url, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data["access_token"]
            print(f"✅ Authentication successful! Token: {access_token[:15]}...")

            # Inject into Session
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            return True
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            return False

    def search_airports(self, keyword="LON"):
        """
        Demo: Use Token to fetch data
        """
        # Lazy load Token
        if "Authorization" not in self.session.headers:
            if not self.get_token():
                return

        print(f"\n✈️ Searching airports for keyword '{keyword}'...")
        url = f"{self.base_url}/reference-data/locations"
        params = {
            "subType": "AIRPORT",
            "keyword": keyword,
            "page[limit]": 5,  # Demo pagination parameter
        }

        response = self.session.get(url, params=params)

        if response.status_code == 200:
            data = response.json().get("data", [])
            for airport in data:
                print(f"   🛫 {airport['name']} ({airport['iataCode']})")
        else:
            print(f"❌ Request failed: {response.status_code} - {response.text}")


if __name__ == "__main__":
    # Replace with your newly applied Key
    API_KEY = os.getenv("AMADEUS_API_KEY")
    API_SECRET = os.getenv("AMADEUS_API_SECRET")

    fetcher = AmadeusFlightFetcher(API_KEY, API_SECRET)
    fetcher.search_airports("PAR")  # Search for Paris airports
