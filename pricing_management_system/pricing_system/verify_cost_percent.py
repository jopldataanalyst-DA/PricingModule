"""Manual diagnostic for cost_into_percent defaults.

Use case:
    Connects directly to MySQL and prints sample stock_items Cost Into % values
    plus a count of rows still using the default 23.0 setting.
"""

from database import get_database

try:
    db = get_database()

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
