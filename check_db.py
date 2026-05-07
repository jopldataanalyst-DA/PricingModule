import sqlite3
DB_PATH = 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db'
conn = sqlite3.connect(DB_PATH)
results = conn.execute('SELECT sku_code FROM stock_items WHERE sku_code LIKE "JOPL%" LIMIT 5').fetchall()
print('JOPL SKUs in DB:', results)
total = conn.execute('SELECT COUNT(*) FROM stock_items').fetchone()[0]
print('Total:', total)
conn.close()