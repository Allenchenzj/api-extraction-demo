import requests
import os 
from dotenv import load_dotenv

load_dotenv()

# Define the API endpoint and your API key
url = "https://api.nasa.gov/planetary/apod"
api_key = os.getenv("NASA_API_KEY")

# 2. Construct request parameters
params = {
    "api_key": api_key,
    "date": "2023-10-01"  # Get image data for a specific date
}

# 3. Send the request
response = requests.get(url, params=params)

# 4. Handle the response
if response.status_code == 200:
    data = response.json()
    print(f"title: {data['title']}")
    print(f"image URL: {data['url']}")
else:
    print(f"Error: {response.status_code}")