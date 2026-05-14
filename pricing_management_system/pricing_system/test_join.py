"""Manual diagnostic for Amazon pricing to stock_items joins.

Use case:
    Uses the shared MySQL helper and prints counts/sample rows to verify that
    amazon_pricing_results.master_sku joins to stock_items.sku_code.
"""

from database import get_database

try:
    db = get_database()

    # Check if tables have data
    apr_count = db.FetchValue("SELECT COUNT(*) as count FROM amazon_pricing_results", Default=0)
    print(f"amazon_pricing_results table has {apr_count} rows")

    si_count = db.FetchValue("SELECT COUNT(*) as count FROM stock_items", Default=0)
    print(f"stock_items table has {si_count} rows")

    # Check sample data
    apr_rows = db.FetchAll("SELECT master_sku FROM amazon_pricing_results LIMIT 3")
    print("Sample master_sku from amazon_pricing_results:")
    for row in apr_rows:
        print(f"  {row['master_sku']}")

    si_rows = db.FetchAll("SELECT sku_code, item_name FROM stock_items LIMIT 3")
    print("Sample sku_code and item_name from stock_items:")
    for row in si_rows:
        print(f"  SKU: {row['sku_code']}, Item: {row['item_name']}")

    # Test the JOIN query used in amazon.py
    rows = db.FetchAll("""
        SELECT apr.master_sku, si.item_name, apr.original_category
        FROM amazon_pricing_results apr
        LEFT JOIN stock_items si ON apr.master_sku = si.sku_code
        LIMIT 5
    """)
    print("Sample data from JOIN query:")
    for row in rows:
        print(f"Master SKU: {row['master_sku']}, Item Name: {row['item_name']}, Category: {row['original_category']}")

except Exception as e:
    print(f"Error: {e}")
