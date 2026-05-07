import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}

def sync_catalog():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Select all Master SKUs from item_master
        cursor.execute("SELECT `Master SKU` FROM item_master")
        item_master_skus = cursor.fetchall()
        
        print(f"Found {len(item_master_skus)} SKUs in item_master")
        
        # Insert each SKU into catalog_pricing if it doesn't exist
        inserted_count = 0
        for (sku,) in item_master_skus:
            if not sku:
                continue
            cursor.execute("""
                INSERT IGNORE INTO catalog_pricing (master_sku, launch_date, catalog_name, cost, wholesale_price, up_price)
                VALUES (%s, NULL, NULL, 0.0, 0.0, 0.0)
            """, (sku,))
            if cursor.rowcount > 0:
                inserted_count += 1
        
        conn.commit()
        print(f"Successfully added {inserted_count} new SKUs to catalog_pricing")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    sync_catalog()
