import sys
sys.path.insert(0, 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system')

# Test admin endpoints
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

# Login first
login_resp = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
print("Login:", login_resp.status_code)
if login_resp.status_code == 200:
    token = login_resp.json()["token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # Test admin/stats
    stats_resp = client.get("/api/admin/stats", headers=headers)
    print("Stats:", stats_resp.status_code, stats_resp.json())
    
    # Test admin/users
    users_resp = client.get("/api/admin/users", headers=headers)
    print("Users:", users_resp.status_code, users_resp.json())