import requests
import os
from dotenv import load_dotenv

load_dotenv()
# 1. Get the Token from .env
token = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")

# 2. Define the Endpoint (Get current user information)
url = "https://api.github.com/user"

# 3. Construct Headers (This is the core of Bearer Token)
headers = {
    "Authorization": f"Bearer {token}",
}

# 4. Send the request (Note there are no params, only headers)
response = requests.get(url, headers=headers)

if response.status_code == 200:
    user_data = response.json()
    print(f"Hello, {user_data['login']}!")
    print(f"Your ID is: {user_data['id']}")
else:
    print(f"Authentication failed: {response.status_code}, {response.text}")