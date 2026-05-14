"""One-off catalog_pricing synchronization script.

Use case:
    Reads Data/CatalogData.csv and upserts pricing/catalog fields by Master SKU
    into the catalog_pricing MySQL table. Run manually when the catalog CSV has
    been updated outside the normal import workflow.
"""

import polars as pl
from database import get_database
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
    
    records = []
    for row in df.to_dicts():
        sku = str(row.get("Master SKU", "")).strip().upper()
        if not sku:
            continue
        records.append({
            "master_sku": sku,
            "launch_date": row.get("Launch Date"),
            "catalog_name": row.get("Catalog Name"),
            "cost": row.get("Cost"),
            "wholesale_price": row.get("Wholesale Price"),
            "up_price": row.get("Up Price"),
            "mrp": row.get("MRP"),
        })

    print(f"Updating catalog_pricing table with {len(records)} records...")
    get_database().UpsertRows("catalog_pricing", records)
    print("Sync complete.")

if __name__ == "__main__":
    sync_catalog()
