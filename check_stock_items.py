import sys
from pathlib import Path

DATABASE_MODULE_DIR = Path(__file__).parent / "pricing_management_system" / "ProcessFiles" / "DatabaseModule"
if str(DATABASE_MODULE_DIR) not in sys.path:
    sys.path.append(str(DATABASE_MODULE_DIR))

from AdvanceDatabase import MySqlDatabase

DB_CONFIG = {"host": "localhost", "user": "root", "password": "123456789", "database": "pricing_module"}

try:
    db = MySqlDatabase(DB_CONFIG, PoolName="CheckStockItemsPool")

    columns = db.FetchAll("DESCRIBE stock_items", Dictionary=False)
    print("=== stock_items table columns ===")
    for col in columns:
        print(f"{col[0]}: {col[1]}")

    column_names = [col[0] for col in columns]
    if "cost_into_percent" not in column_names:
        print("\nAdding cost_into_percent column to stock_items table...")
        db.ExecuteQuery("ALTER TABLE stock_items ADD COLUMN cost_into_percent FLOAT DEFAULT 23.0")
        print("Column added to stock_items table")
    else:
        print("\nColumn cost_into_percent already exists in stock_items")

except Exception as e:
    print(f"Error: {e}")
