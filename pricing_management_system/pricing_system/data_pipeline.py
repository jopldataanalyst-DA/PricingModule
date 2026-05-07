import polars as pl
import requests
from io import BytesIO
from pathlib import Path
from datetime import datetime
from database import init_db, get_db

# Setup Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR.parent.parent / "Data"
# ITEM_MASTER_CSV = DATA_DIR / "ItemMaster.csv" # No longer used

STOCK_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"

def clean_stock_data(url: str) -> pl.DataFrame:
    response = requests.get(url)
    response.raise_for_status()
    raw_df = pl.read_csv(BytesIO(response.content), has_header=False)
    df = raw_df.slice(3)
    new_columns = ["uni", "uni_stock", "fba", "fba_stock", "fbf", "fbf_stock", "sjit", "sjit_stock"]
    df.columns = new_columns
    df = df.filter(pl.col("uni").is_not_null())
    df = df.with_columns([
        pl.col("uni").cast(pl.Utf8).str.strip_chars(),
        pl.col("fba").cast(pl.Utf8).str.strip_chars(),
        pl.col("fbf").cast(pl.Utf8).str.strip_chars(),
        pl.col("sjit").cast(pl.Utf8).str.strip_chars(),
        pl.col("uni_stock").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("fba_stock").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("fbf_stock").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("sjit_stock").cast(pl.Int64, strict=False).fill_null(0),
    ])
    return df

def run_pipeline():
    init_db()
    print(f"[{datetime.now()}] Starting data pipeline upgrade...")
    
    # 1. Fetch & Clean Stock Data
    stock_raw = clean_stock_data(STOCK_URL)
    stock_raw.columns = [
        "Uniware SKU", "Uniware Stock",
        "FBA SKU", "FBA",
        "FBF SKU", "FBF",
        "SJIT SKU", "SJIT"
    ]
    
    stock = stock_raw.with_columns([
        pl.col(c).cast(pl.Utf8).str.strip_chars()
        for c in ["Uniware SKU", "FBA SKU", "FBF SKU", "SJIT SKU"]
    ] + [
        pl.col(c).cast(pl.Int64, strict=False).fill_null(0)
        for c in ["Uniware Stock", "FBA", "FBF", "SJIT"]
    ])

    def make_stock_df(sku_col, stock_col):
        return (
            stock
            .select([pl.col(sku_col).alias("Master SKU"), pl.col(stock_col)])
            .filter(
                pl.col("Master SKU").is_not_null() &
                (pl.col("Master SKU") != "") &
                (pl.col("Master SKU").str.to_lowercase() != "uni") &
                (pl.col("Master SKU").str.to_lowercase() != "paste here :-")
            )
            .group_by("Master SKU")
            .agg(pl.col(stock_col).sum())
        )

    stock_tables = [
        make_stock_df("Uniware SKU", "Uniware Stock"),
        make_stock_df("FBA SKU", "FBA"),
        make_stock_df("FBF SKU", "FBF"),
        make_stock_df("SJIT SKU", "SJIT"),
    ]

    # Merge all stock tables into one
    stock_merged = stock_tables[0]
    for table in stock_tables[1:]:
        stock_merged = stock_merged.join(table, on="Master SKU", how="full", coalesce=True)
    
    stock_merged = stock_merged.select([
        pl.col("Master SKU").alias("master_sku"),
        pl.col("Uniware Stock").fill_null(0).alias("uniware_stock"),
        pl.col("FBA").fill_null(0).alias("fba_stock"),
        pl.col("FBF").fill_null(0).alias("fbf_stock"),
        pl.col("SJIT").fill_null(0).alias("sjit_stock")
    ])

    # 2. Save stock_update to Database
    print(f"[{datetime.now()}] Saving stock_update to database...")
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        stock_records = stock_merged.to_dicts()
        for row in stock_records:
            cursor.execute("""
                INSERT INTO stock_update (master_sku, uniware_stock, fba_stock, fbf_stock, sjit_stock)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    uniware_stock=VALUES(uniware_stock),
                    fba_stock=VALUES(fba_stock),
                    fbf_stock=VALUES(fbf_stock),
                    sjit_stock=VALUES(sjit_stock)
            """, (
                row["master_sku"], row["uniware_stock"], 
                row["fba_stock"], row["fbf_stock"], row["sjit_stock"]
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[{datetime.now()}] stock_update table updated with {len(stock_records)} records.")
    except Exception as e:
        print(f"[{datetime.now()}] Error saving to stock_update: {e}")
        return

    # 3. Fetch item_master, stock_update, and catalog_pricing from Database for final dashboard
    print(f"[{datetime.now()}] Generating final dashboard from database tables...")
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        # Fetch item_master
        cursor.execute("SELECT * FROM item_master")
        item_master_df = pl.from_dicts(cursor.fetchall())
        
        # Fetch stock_update
        cursor.execute("SELECT * FROM stock_update")
        stock_update_df = pl.from_dicts(cursor.fetchall())
        
        # Fetch catalog_pricing with explicit schema for strings to avoid inference errors
        cursor.execute("SELECT * FROM catalog_pricing")
        catalog_pricing_rows = cursor.fetchall()
        if catalog_pricing_rows:
            catalog_pricing_df = pl.from_dicts(catalog_pricing_rows, schema_overrides={
                "launch_date": pl.String,
                "catalog_name": pl.String
            })
        else:
            # Fallback for empty table
            catalog_pricing_df = pl.DataFrame(schema={
                "master_sku": pl.String, "launch_date": pl.String, "catalog_name": pl.String,
                "cost": pl.Float64, "wholesale_price": pl.Float64, "up_price": pl.Float64
            })
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[{datetime.now()}] Error fetching tables from DB: {e}")
        return

    # Process item master
    item = item_master_df.with_columns(
        pl.col("Master SKU").cast(pl.Utf8).str.strip_chars()
    ).rename({"Loc": "Location"})

    # Join with stock data
    final = item.join(
        stock_update_df.select([
            "master_sku", "uniware_stock", "fba_stock", "fbf_stock", "sjit_stock"
        ]).rename({
            "master_sku": "Master SKU",
            "uniware_stock": "Uniware Stock",
            "fba_stock": "FBA",
            "fbf_stock": "FBF",
            "sjit_stock": "SJIT"
        }), 
        on="Master SKU", 
        how="left"
    )

    # Join with catalog pricing data
    final = final.join(
        catalog_pricing_df.select([
            "master_sku", "launch_date", "catalog_name", "cost", "wholesale_price", "up_price"
        ]).rename({
            "master_sku": "Master SKU",
            "launch_date": "Launch Date DB",
            "catalog_name": "Catalog Name DB",
            "cost": "Cost DB",
            "wholesale_price": "Wholesale Price DB",
            "up_price": "Up Price DB"
        }),
        on="Master SKU",
        how="left"
    )


    final = final.with_columns([
        pl.col("Cost DB").fill_null(0.0).alias("Cost"),
        pl.col("Wholesale Price DB").fill_null(0.0).alias("Wholesale Price"),
        pl.col("Catalog Name DB").fill_null("").alias("Catalog Name"),
        pl.col("Up Price DB").fill_null(0.0).alias("Up Price"),
        pl.col("Launch Date DB").fill_null('').alias("Launch Date"),
        *[pl.col(c).fill_null(0) for c in ["Uniware Stock", "FBA", "FBF", "SJIT"]]
    ]).select([
        "Master SKU", "Style ID / Parent SKU", "Size", "Category", "Location",
        "Catalog Name", "Cost", "Wholesale Price", "Up Price", 
        "Uniware Stock", "FBA", "FBF", "SJIT", "Launch Date"
    ])

    # 4. Save to final dashboard table (stock_items)
    final_db_ready = final.rename({
        "Master SKU": "sku_code", "Style ID / Parent SKU": "item_name",
        "Size": "size", "Category": "category", "Location": "location",
        "Cost": "cost", "Wholesale Price": "price", "Catalog Name": "catalog",
        "Up Price": "mrp", "Uniware Stock": "available_atp", "FBA": "fba_stock",
        "FBF": "fbf_stock", "SJIT": "sjit_stock", "Launch Date": "updated"
    })

    records = final_db_ready.to_dicts()
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Truncate stock_items to ensure deleted source records are removed from dashboard
        cursor.execute("TRUNCATE TABLE stock_items")
        
        for row in records:
            cursor.execute("""
                INSERT INTO stock_items 
                (sku_code, item_name, size, category, location, catalog, cost, price, mrp, available_atp, fba_stock, fbf_stock, sjit_stock, updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    item_name=VALUES(item_name),
                    size=VALUES(size),
                    category=VALUES(category),
                    location=VALUES(location),
                    catalog=VALUES(catalog),
                    cost=VALUES(cost),
                    price=VALUES(price),
                    mrp=VALUES(mrp),
                    available_atp=VALUES(available_atp),
                    fba_stock=VALUES(fba_stock),
                    fbf_stock=VALUES(fbf_stock),
                    sjit_stock=VALUES(sjit_stock),
                    updated=VALUES(updated)
            """, (
                row.get("sku_code"), row.get("item_name"), row.get("size"), 
                row.get("category"), row.get("location"), row.get("catalog"), 
                float(row.get("cost", 0) or 0), float(row.get("price", 0) or 0), 
                float(row.get("mrp", 0) or 0), int(row.get("available_atp", 0) or 0), 
                int(row.get("fba_stock", 0) or 0), int(row.get("fbf_stock", 0) or 0), 
                int(row.get("sjit_stock", 0) or 0), row.get("updated")
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[{datetime.now()}] Pipeline complete! Processed {len(records)} records into stock_items.")
    except Exception as e:
        print(f"[{datetime.now()}] Error updating database: {e}")

if __name__ == "__main__":
    run_pipeline()

