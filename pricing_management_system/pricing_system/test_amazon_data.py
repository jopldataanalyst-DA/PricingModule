"""Manual smoke test for Amazon pricing API data.

Use case:
    Logs into a local FastAPI server as admin and verifies that Amazon pricing
    data includes joined item_name values. This is a script-style check, not a
    pytest test.
"""

import requests

BASE_URL = "http://localhost:8000"
USERNAME = "admin"
PASSWORD = "admin123"

# Login
login_response = requests.post(f"{BASE_URL}/auth/login", json={
    "username": USERNAME,
    "password": PASSWORD
})

if login_response.status_code != 200:
    print("Login failed")
    exit(1)

login_data = login_response.json()
token = login_data.get('token')

if not token:
    print("No token in login response")
    exit(1)

headers = {"Authorization": f"Bearer {token}"}

# Test data endpoint
data_response = requests.get(f"{BASE_URL}/api/amazon/data?page=1&limit=5", headers=headers)

if data_response.status_code != 200:
    print(f"Data API failed: {data_response.status_code}")
    print(data_response.text)
    exit(1)

data = data_response.json()

print("Sample Amazon pricing data:")
for item in data.get('data', [])[:3]:
    print(f"Master SKU: {item.get('master_sku')}, Style ID: {item.get('item_name')}, Category: {item.get('original_category')}")

if any(item.get('item_name') for item in data.get('data', [])):
    print("✅ item_name data is being returned in the API!")
else:
    print("❌ item_name data is missing from API response")
