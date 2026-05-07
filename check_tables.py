import sqlite3
conn = sqlite3.connect('D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db')
print('Tables:', conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())
conn.close()