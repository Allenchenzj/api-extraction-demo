import os
import requests
from dotenv import load_dotenv

# 1. Create a .env file and paste your Read Access Token there
# Save the variable as TMDB_READ_ACCESS_TOKEN
load_dotenv()


class TMDBClient:
    def __init__(self):
        self.base_url = "https://api.themoviedb.org/3"
        self.token = os.getenv("TMDB_READ_ACCESS_TOKEN")

        # 2. Core encapsulation: Create Session
        self.session = requests.Session()

        # 3. Core encapsulation: Inject Bearer Token
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.session.headers.get('Authorization', self.token)}",
                "Accept": "application/json",
                "User-Agent": "MovieDataProject/1.0",
            }
        )

    def test_connection(self):
        """Test if connection is successful"""
        # Use a simple endpoint to test: get popular movies
        endpoint = "/movie/popular"
        response = self.session.get(f"{self.base_url}{endpoint}")

        if response.status_code == 200:
            print("✅ Authentication successful! Headers configured correctly.")
            print(response.text)
            print(f"Current quota/status: {response.status_code}")
        else:
            print(f"❌ Authentication failed, status code: {response.status_code}")
            print(response.text)


# Instantiate and test
client = TMDBClient()
client.test_connection()
