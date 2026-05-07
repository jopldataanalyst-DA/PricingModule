import sqlite3
DB_PATH = 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db'
conn = sqlite3.connect(DB_PATH)

results = conn.execute('SELECT sku_code, category, location, item_name FROM stock_items WHERE sku_code LIKE "JOPL%" LIMIT 10').fetchall()
print('First 10 JOPL items:')
for r in results:
    print(r)

print()
# Count items with category
count = conn.execute('SELECT COUNT(*) FROM stock_items WHERE category IS NOT NULL AND category != ""').fetchone()[0]
print(f'Items with Category: {count}')

conn.close()