import sqlite3, os

paths = [
    'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db',
    'D:/VatsalFiles/PricingModule/Data/pricing.db',
    'D:/VatsalFiles/PricingModule/pricing.db',
]

for p in paths:
    exists = os.path.exists(p)
    print(f'{exists}: {p}')
    if exists:
        conn = sqlite3.connect(p)
        try:
            count = conn.execute('SELECT COUNT(*) FROM stock_items').fetchone()[0]
            print(f'  Rows: {count}')
            # Sample
            sample = conn.execute('SELECT sku_code FROM stock_items LIMIT 1').fetchone()
            print(f'  Sample: {sample}')
        except Exception as e:
            print(f'  Error: {e}')
        conn.close()