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

    # Check stock_items table structure
    cursor.execute("DESCRIBE stock_items")
    columns = cursor.fetchall()
    print("=== stock_items table columns ===")
    for col in columns:
        print(f"{col[0]}: {col[1]}")

    # Check if cost_into_percent column exists
    column_names = [col[0] for col in columns]
    if 'cost_into_percent' not in column_names:
        print("\nAdding cost_into_percent column to stock_items table...")
        cursor.execute("ALTER TABLE stock_items ADD COLUMN cost_into_percent FLOAT DEFAULT 23.0")
        conn.commit()
        print("✅ Column added to stock_items table")
    else:
        print("\nColumn cost_into_percent already exists in stock_items")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")