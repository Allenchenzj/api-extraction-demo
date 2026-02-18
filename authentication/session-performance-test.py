import requests
import time

URL = "https://httpbin.org/get"
COUNT = 50  # Number of requests to send

print(f"Preparing to start the test, sending {COUNT} requests each...\n")

# === Competitor 1: Regular requests (no Session) ===
start_time = time.time()

for i in range(COUNT):
    # A new connection is established each time (Handshake)
    requests.get(URL)

end_time = time.time()
no_session_time = end_time - start_time
print(f"Regular requests took: {no_session_time:.2f} seconds")


# === Competitor 2: Using Session (Connection Reuse) ===
start_time = time.time()

# Using a context manager (with) is best practice to automatically close the connection when done
with requests.Session() as s:
    for i in range(COUNT):
        # Reuse the previous connection
        s.get(URL)

end_time = time.time()
session_time = end_time - start_time
print(f"Session mode took: {session_time:.2f} seconds")