import sqlite3

paths = [
    'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db',
    'D:/VatsalFiles/PricingModule/pricing.db',
    'D:/VatsalFiles/PricingModule/Data/pricing.db',
]

for p in paths:
    try:
        conn = sqlite3.connect(p)
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if tables:
            count = conn.execute('SELECT COUNT(*) FROM stock_items').fetchone()[0]
            print(f'{p}: {count} rows')
        conn.close()
    except Exception as e:
        print(f'{p}: error - {e}')