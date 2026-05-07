import sys
sys.path.insert(0, 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system')

# Test admin imports
from admin import router
print("admin router:", router.routes[:3])

# Test items import
from items import load_items_csv
items = load_items_csv()
print(f"Items: {len(items)}")

# Test get_current_user import in auth
from auth import get_current_user
print("auth OK")

# Test database
from database import load_users
users = load_users()
print(f"Users: {len(users)}")