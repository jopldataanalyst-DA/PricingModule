"""Manual diagnostic for cost_into_percent defaults.

Use case:
    Connects directly to MySQL and prints sample stock_items Cost Into % values
    plus a count of rows still using the default 23.0 setting.
"""

import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Check sample records from stock_items
    cursor.execute("SELECT sku_code, cost_into_percent FROM stock_items LIMIT 10")
    stock_items = cursor.fetchall()

    print("=== stock_items cost_into_percent values ===")
    for row in stock_items:
        print(f"{row[0]}: {row[1]}")

    # Check if all records have the value
    cursor.execute("SELECT COUNT(*) as total FROM stock_items WHERE cost_into_percent = 23.0")
    count_23 = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) as total FROM stock_items")
    total = cursor.fetchone()[0]

    print(f"\nRecords with cost_into_percent = 23.0: {count_23}")
    print(f"Total records: {total}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
