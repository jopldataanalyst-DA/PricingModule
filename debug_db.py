import sqlite3, os

p = 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db'
print(f'Size: {os.path.getsize(p)}')

conn = sqlite3.connect(p)
conn.row_factory = sqlite3.Row

# Check tables
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print(f'Tables: {[t[0] for t in tables]}')

# Check count
count = conn.execute('SELECT COUNT(*) FROM stock_items').fetchone()[0]
print(f'stock_items count: {count}')

# Sample
if count > 0:
    sample = conn.execute('SELECT * FROM stock_items LIMIT 1').fetchone()
    print(f'Sample: {dict(sample)}')

conn.close()