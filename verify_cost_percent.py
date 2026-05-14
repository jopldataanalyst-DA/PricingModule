import sys
from pathlib import Path

DATABASE_MODULE_DIR = Path(__file__).parent / "pricing_management_system" / "ProcessFiles" / "DatabaseModule"
if str(DATABASE_MODULE_DIR) not in sys.path:
    sys.path.append(str(DATABASE_MODULE_DIR))

from AdvanceDatabase import MySqlDatabase

DB_CONFIG = {"host": "localhost", "user": "root", "password": "123456789", "database": "pricing_module"}

try:
    db = MySqlDatabase(DB_CONFIG, PoolName="VerifyCostPercentPool")

    # Check sample records from stock_items
    stock_items = db.FetchAll("SELECT sku_code, cost_into_percent FROM stock_items LIMIT 10", Dictionary=False)

    print("=== stock_items cost_into_percent values ===")
    for row in stock_items:
        print(f"{row[0]}: {row[1]}")

    # Check if all records have the value
    count_23 = db.FetchValue("SELECT COUNT(*) as total FROM stock_items WHERE cost_into_percent = 23.0", Default=0)
    total = db.FetchValue("SELECT COUNT(*) as total FROM stock_items", Default=0)

    print(f"\nRecords with cost_into_percent = 23.0: {count_23}")
    print(f"Total records: {total}")

except Exception as e:
    print(f"Error: {e}")
