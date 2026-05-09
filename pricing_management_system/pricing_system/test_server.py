import requests
import time

BASE_URL = "http://localhost:8000"

# Wait for server to be ready
for i in range(10):
    try:
        response = requests.get(f"{BASE_URL}/docs", timeout=5)
        if response.status_code == 200:
            print("Server is ready!")
            break
    except:
        print(f"Waiting for server... ({i+1}/10)")
        time.sleep(2)
else:
    print("Server not ready after 20 seconds")
    exit(1)

# Test login
login_response = requests.post(f"{BASE_URL}/auth/login", json={
    "username": "admin",
    "password": "admin123"
}, timeout=10)

print(f"Login status: {login_response.status_code}")
if login_response.status_code == 200:
    print("Login successful!")
    login_data = login_response.json()
    token = login_data.get('token')
    print(f"Got token: {token is not None}")

    # Test columns API
    headers = {"Authorization": f"Bearer {token}"}
    columns_response = requests.get(f"{BASE_URL}/api/amazon/columns", headers=headers, timeout=10)
    print(f"Columns API status: {columns_response.status_code}")

    if columns_response.status_code == 200:
        data = columns_response.json()
        visible = data.get('visible', [])
        if 'item_name' in visible:
            pos = visible.index('item_name')
            print(f"✅ item_name found at position {pos} in visible columns")
        else:
            print("❌ item_name not found in visible columns")
else:
    print(f"Login failed: {login_response.text}")