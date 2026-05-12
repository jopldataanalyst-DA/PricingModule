"""One-off catalog_pricing synchronization script.

Use case:
    Reads Data/CatalogData.csv and upserts pricing/catalog fields by Master SKU
    into the catalog_pricing MySQL table. Run manually when the catalog CSV has
    been updated outside the normal import workflow.
"""

import polars as pl
from database import get_db
from pathlib import Path

DATA_DIR = Path(r"D:\VatsalFiles\PricingModule\Data")
CATALOG_CSV = DATA_DIR / "CatalogData.csv"

def sync_catalog():
    """Load CatalogData.csv and upsert each SKU into catalog_pricing."""
    if not CATALOG_CSV.exists():
        print(f"File not found: {CATALOG_CSV}")
        return

    print(f"Reading {CATALOG_CSV}...")
    # Read all as strings first to avoid parsing errors with 'Pending', '.', etc.
    df = pl.read_csv(CATALOG_CSV, infer_schema_length=0)
    
    # Clean and cast numeric columns
    numeric_cols = ["MRP", "Cost", "Wholesale Price", "Up Price"]
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0)
            )
    
    records = df.to_dicts()
    
    conn = get_db()
    cursor = conn.cursor()
    
    print(f"Updating catalog_pricing table with {len(records)} records...")
    for row in records:
        sku = str(row.get("Master SKU", "")).strip().upper()
        if not sku:
            continue
            
        cursor.execute("""
            INSERT INTO catalog_pricing (master_sku, launch_date, catalog_name, cost, wholesale_price, up_price, mrp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                launch_date=VALUES(launch_date),
                catalog_name=VALUES(catalog_name),
                cost=VALUES(cost),
                wholesale_price=VALUES(wholesale_price),
                up_price=VALUES(up_price),
                mrp=VALUES(mrp)
        """, (
            sku,
            row.get("Launch Date"),
            row.get("Catalog Name"),
            row.get("Cost"),
            row.get("Wholesale Price"),
            row.get("Up Price"),
            row.get("MRP")
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Sync complete.")

if __name__ == "__main__":
    sync_catalog()
