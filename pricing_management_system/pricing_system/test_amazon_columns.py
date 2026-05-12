"""Manual smoke test for Amazon pricing column metadata.

Use case:
    Logs into the local API and prints /api/amazon/columns so a developer can
    confirm the frontend will receive item_name and other expected columns.
"""

import requests
import json

# First login to get token
login_url = "http://localhost:8000/api/auth/login"
api_url = "http://localhost:8000/api/amazon/columns"

login_data = {
    "username": "admin",
    "password": "admin123"
}

try:
    # Login
    login_response = requests.post(login_url, json=login_data)

    if login_response.status_code == 200:
        login_data = login_response.json()
        token = login_data.get('token')

        if token:
            print("Login successful, got token")

            # Get columns with token
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(api_url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                print("API Response:")
                print(json.dumps(data, indent=2))

                # Check if item_name is in the columns
                if 'visible' in data and 'item_name' in data['visible']:
                    print("\n✅ item_name column found in visible columns!")
                    position = data['visible'].index('item_name')
                    print(f"Position in visible columns: {position} (right after master_sku)")
                else:
                    print("\n❌ item_name column NOT found in visible columns")

                if 'all' in data and 'item_name' in data['all']:
                    print("✅ item_name column found in all columns!")
                else:
                    print("❌ item_name column NOT found in all columns")

            else:
                print(f"API Error: {response.status_code}")
                print(response.text)
        else:
            print("No token in login response")
            print(login_data)
    else:
        print(f"Login failed: {login_response.status_code}")
        print(login_response.text)

except Exception as e:
    print(f"Error: {e}")
