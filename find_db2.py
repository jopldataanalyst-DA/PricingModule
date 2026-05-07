import sqlite3
import sys
sys.path.insert(0, 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system')
from database import DB_PATH
print('DB_PATH:', DB_PATH)
conn = sqlite3.connect(DB_PATH)
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print('Tables:', tables)
if tables:
    count = conn.execute('SELECT COUNT(*) FROM stock_items').fetchone()[0]
    print('Rows:', count)
conn.close()