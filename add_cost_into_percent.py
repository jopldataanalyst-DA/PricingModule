import sys
from pathlib import Path

DATABASE_MODULE_DIR = Path(__file__).parent / "pricing_management_system" / "ProcessFiles" / "DatabaseModule"
if str(DATABASE_MODULE_DIR) not in sys.path:
    sys.path.append(str(DATABASE_MODULE_DIR))

from AdvanceDatabase import MySqlDatabase

DB_CONFIG = {"host": "localhost", "user": "root", "password": "123456789", "database": "pricing_module"}

try:
    db = MySqlDatabase(DB_CONFIG, PoolName="AddCostPercentPool")

    columns = [row[0] for row in db.FetchAll("DESCRIBE catalog_pricing", Dictionary=False)]

    if "cost_into_percent" not in columns:
        print("Adding cost_into_percent column to catalog_pricing table...")
        db.ExecuteQuery("""
            ALTER TABLE catalog_pricing
            ADD COLUMN cost_into_percent FLOAT DEFAULT 23.0
        """)
        db.ExecuteQuery("""
            UPDATE catalog_pricing
            SET cost_into_percent = 23.0
        """)

        print("Column added successfully and all existing records updated to 23.0")

        total_records = db.FetchValue("SELECT COUNT(*) as total FROM catalog_pricing", Default=0)
        print(f"Total records in catalog_pricing: {total_records}")

        samples = db.FetchAll("SELECT master_sku, cost_into_percent FROM catalog_pricing LIMIT 5", Dictionary=False)
        print("\nSample records:")
        for row in samples:
            print(f"  {row[0]}: {row[1]}")

    else:
        print("Column cost_into_percent already exists in catalog_pricing table")

except Exception as e:
    print(f"Error: {e}")
