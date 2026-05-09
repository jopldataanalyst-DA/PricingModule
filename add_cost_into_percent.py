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

    # Check if column already exists
    cursor.execute("DESCRIBE catalog_pricing")
    columns = [row[0] for row in cursor.fetchall()]

    if 'cost_into_percent' not in columns:
        print("Adding cost_into_percent column to catalog_pricing table...")

        # Add the column with default value 23
        cursor.execute("""
            ALTER TABLE catalog_pricing
            ADD COLUMN cost_into_percent FLOAT DEFAULT 23.0
        """)

        # Update all existing records to have value 23
        cursor.execute("""
            UPDATE catalog_pricing
            SET cost_into_percent = 23.0
        """)

        conn.commit()
        print("✅ Column added successfully and all existing records updated to 23.0")

        # Verify the changes
        cursor.execute("SELECT COUNT(*) as total FROM catalog_pricing")
        total_records = cursor.fetchone()[0]
        print(f"Total records in catalog_pricing: {total_records}")

        # Check a few sample records
        cursor.execute("SELECT master_sku, cost_into_percent FROM catalog_pricing LIMIT 5")
        samples = cursor.fetchall()
        print("\nSample records:")
        for row in samples:
            print(f"  {row[0]}: {row[1]}")

    else:
        print("Column cost_into_percent already exists in catalog_pricing table")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")