import requests
import os
from dotenv import load_dotenv

load_dotenv()
# Step to authenticate and fetch data from Amadeus API

auth_url = "https://test.api.amadeus.com/v1/security/oauth2/token"
client_id = os.getenv("AMADEUS_API_KEY")
client_secret = os.getenv("AMADEUS_API_SECRET")
# Notice: OAuth token retrieval is usually a POST request with sensitive info in the body (data)
auth_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret
}

print("Getting Access Token...")
auth_response = requests.post(auth_url, data=auth_data)
access_token = auth_response.json().get("access_token")
print(f"Got Token: {access_token[:10]}...") # Only print the first 10 characters for demonstration

# Step to fetch data (use the token to get data) ===

# Example: Search for flight offers from CDG to LHR on a specific date
data_url = "https://test.api.amadeus.com/v2/shopping/flight-offers"
params = {
    "originLocationCode": "CDG",
    "destinationLocationCode": "LHR",
    "departureDate": "2026-02-10",
    "adults": "1",
    "max": "3"
}

# Put the token we just got into the Header
headers = {
    "Authorization": f"Bearer {access_token}"
}

print("Fetching flight data...")
# data_response = requests.get(data_url, headers=headers, params=params)

# if data_response.status_code == 200:
#     flights = data_response.json()
#     print(f"Successfully retrieved {len(flights['data'])} flight offers!")
#     # Print the price of the first flight offer
#     print(f"First flight price: {flights['data'][0]['price']['total']} EUR")
# else:
#     print(f"Failed: {data_response.status_code}, {data_response.text}")

s = requests.Session()
s.headers.update(headers)
dates = ["2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13", "2026-02-14"]
for date in dates:
    params["departureDate"] = date
    date_response = s.get(data_url, params=params)

    if date_response.status_code == 200:
        flights = date_response.json()
        print(f"Date: {date} - Found {len(flights.get('data', []))} flights")
    else:
        print(f"Date: {date} - Failed with status: {date_response.status_code}")
s.close()