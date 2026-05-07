import sqlite3
DB_PATH = 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db'
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Get one full row to see all fields
result = conn.execute('SELECT * FROM stock_items WHERE sku_code = "JOPLAI1001D"').fetchone()
if result:
    d = dict(result)
    print('Full row for JOPLAI1001D:')
    for k, v in d.items():
        if v is not None and v != '':
            print(f'  {k}: {v}')
else:
    print('Not found')

conn.close()