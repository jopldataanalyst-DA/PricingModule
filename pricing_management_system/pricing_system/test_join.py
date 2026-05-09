import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Check if tables have data
    cursor.execute("SELECT COUNT(*) as count FROM amazon_pricing_results")
    apr_count = cursor.fetchone()['count']
    print(f"amazon_pricing_results table has {apr_count} rows")

    cursor.execute("SELECT COUNT(*) as count FROM stock_items")
    si_count = cursor.fetchone()['count']
    print(f"stock_items table has {si_count} rows")

    # Check sample data
    cursor.execute("SELECT master_sku FROM amazon_pricing_results LIMIT 3")
    apr_rows = cursor.fetchall()
    print("Sample master_sku from amazon_pricing_results:")
    for row in apr_rows:
        print(f"  {row['master_sku']}")

    cursor.execute("SELECT sku_code, item_name FROM stock_items LIMIT 3")
    si_rows = cursor.fetchall()
    print("Sample sku_code and item_name from stock_items:")
    for row in si_rows:
        print(f"  SKU: {row['sku_code']}, Item: {row['item_name']}")

    # Test the JOIN query used in amazon.py
    cursor.execute("""
        SELECT apr.master_sku, si.item_name, apr.original_category
        FROM amazon_pricing_results apr
        LEFT JOIN stock_items si ON apr.master_sku = si.sku_code
        LIMIT 5
    """)

    rows = cursor.fetchall()
    print("Sample data from JOIN query:")
    for row in rows:
        print(f"Master SKU: {row['master_sku']}, Item Name: {row['item_name']}, Category: {row['original_category']}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")