import sqlite3
# Check multiple possible paths
paths = [
    'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db',
    'D:/VatsalFiles/PricingModule/Data/pricing.db',
    'D:/VatsalFiles/PricingModule/pricing_management_system/data/pricing.db',
    'pricing.db',
]
for p in paths:
    try:
        conn = sqlite3.connect(p)
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if tables:
            print(f'{p}: {tables}')
        conn.close()
    except Exception as e:
        print(f'{p}: error - {e}')