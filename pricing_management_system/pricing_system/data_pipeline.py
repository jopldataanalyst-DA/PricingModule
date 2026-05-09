import polars as pl
import requests
from io import BytesIO
from pathlib import Path
from datetime import datetime
from database import init_db, get_db
import sys

# Setup Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR.parent.parent / "Data"
AMAZON_DIR = BASE_DIR.parent / "ProcessFiles" / "AmazonPricingModule"
sys.path.append(str(AMAZON_DIR))
from AmazonPricing import run_amazon_pricing
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
        pl.col(c).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
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

    # 3. Fetch stock_items, stock_update, and catalog_pricing from Database for final dashboard
    print(f"[{datetime.now()}] Generating final dashboard from database tables...")
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        # Fetch stock_items (this IS the item master – populated by CSV import)
        cursor.execute("SELECT * FROM stock_items")
        stock_items_rows = cursor.fetchall()
        if not stock_items_rows:
            print(f"[{datetime.now()}] stock_items table is empty – skipping dashboard rebuild.")
            cursor.close()
            conn.close()
        else:
            stock_items_df = pl.from_dicts(stock_items_rows, schema_overrides={
                "updated": pl.String,
                "child_remark": pl.String,
                "parent_remark": pl.String,
                "item_type": pl.String,
            })

            # Fetch stock_update
            cursor.execute("SELECT * FROM stock_update")
            stock_update_rows = cursor.fetchall()
            if stock_update_rows:
                stock_update_df = pl.from_dicts(stock_update_rows).with_columns(
                    pl.col("master_sku").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
                )
            else:
                stock_update_df = pl.DataFrame(schema={
                    "master_sku": pl.String, "uniware_stock": pl.Int64,
                    "fba_stock": pl.Int64, "fbf_stock": pl.Int64, "sjit_stock": pl.Int64
                })

            # Fetch catalog_pricing
            cursor.execute("SELECT * FROM catalog_pricing")
            catalog_pricing_rows = cursor.fetchall()
            if catalog_pricing_rows:
                catalog_pricing_df = pl.from_dicts(catalog_pricing_rows, schema_overrides={
                    "launch_date": pl.String,
                    "catalog_name": pl.String
                }).with_columns(
                    pl.col("master_sku").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
                )
            else:
                catalog_pricing_df = pl.DataFrame(schema={
                    "master_sku": pl.String, "launch_date": pl.String, "catalog_name": pl.String,
                    "cost": pl.Float64, "wholesale_price": pl.Float64, "up_price": pl.Float64,
                    "mrp": pl.Float64
                })

            cursor.close()
            conn.close()

            # Normalise the sku_code column so joins work reliably
            stock_items_df = stock_items_df.with_columns(
                pl.col("sku_code").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
            )

            # Merge fresh stock totals from stock_update into stock_items
            stock_items_df = stock_items_df.join(
                stock_update_df.select([
                    "master_sku", "uniware_stock", "fba_stock", "fbf_stock", "sjit_stock"
                ]).rename({
                    "master_sku": "sku_code",
                    "uniware_stock": "_new_uni",
                    "fba_stock": "_new_fba",
                    "fbf_stock": "_new_fbf",
                    "sjit_stock": "_new_sjit",
                }),
                on="sku_code",
                how="left"
            ).with_columns([
                pl.coalesce(["_new_uni",  "available_atp"]).fill_null(0).alias("available_atp"),
                pl.coalesce(["_new_fba",  "fba_stock"]).fill_null(0).alias("fba_stock"),
                pl.coalesce(["_new_fbf",  "fbf_stock"]).fill_null(0).alias("fbf_stock"),
                pl.coalesce(["_new_sjit", "sjit_stock"]).fill_null(0).alias("sjit_stock"),
            ]).drop(["_new_uni", "_new_fba", "_new_fbf", "_new_sjit"])

            # Merge catalog pricing (cost / mrp / launch_date / cost_into_percent) where not already set
            stock_items_df = stock_items_df.join(
                catalog_pricing_df.select([
                    "master_sku", "cost", "mrp", "launch_date", "cost_into_percent"
                ]).rename({
                    "master_sku": "sku_code",
                    "cost": "_cat_cost",
                    "mrp": "_cat_mrp",
                    "launch_date": "_cat_launch",
                    "cost_into_percent": "_cat_cost_percent",
                }),
                on="sku_code",
                how="left"
            ).with_columns([
                pl.coalesce(["_cat_cost", "cost"]).fill_null(0.0).alias("cost"),
                pl.coalesce(["_cat_mrp",  "mrp"]).fill_null(0.0).alias("mrp"),
                pl.coalesce(["_cat_launch", "updated"]).fill_null("").alias("updated"),
                pl.coalesce(["_cat_cost_percent", pl.lit(23.0)]).fill_null(23.0).alias("cost_into_percent"),
            ]).drop(["_cat_cost", "_cat_mrp", "_cat_launch", "_cat_cost_percent"])

            # Persist updated values back into stock_items
            records = stock_items_df.to_dicts()
            try:
                conn2 = get_db()
                cursor2 = conn2.cursor()
                cursor2.execute("TRUNCATE TABLE stock_items")
                for row in records:
                    cursor2.execute("""
                        INSERT INTO stock_items
                        (sku_code, item_name, size, category, location, child_remark, parent_remark,
                         item_type, catalog, cost, price, mrp, up_price, cost_into_percent,
                         available_atp, fba_stock, fbf_stock, sjit_stock, updated)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        row.get("sku_code"), row.get("item_name"), row.get("size"),
                        row.get("category"), row.get("location"), row.get("child_remark"),
                        row.get("parent_remark"), row.get("item_type"), row.get("catalog"),
                        float(row.get("cost") or 0), float(row.get("price") or 0),
                        float(row.get("mrp") or 0), float(row.get("up_price") or 0),
                        float(row.get("cost_into_percent") or 23.0),
                        int(row.get("available_atp") or 0),
                        int(row.get("fba_stock") or 0), int(row.get("fbf_stock") or 0),
                        int(row.get("sjit_stock") or 0), row.get("updated")
                    ))
                conn2.commit()
                cursor2.close()
                conn2.close()
                print(f"[{datetime.now()}] stock_items refreshed with {len(records)} records.")
            except Exception as e:
                print(f"[{datetime.now()}] Error refreshing stock_items: {e}")
                return

    except Exception as e:
        print(f"[{datetime.now()}] Error in dashboard rebuild step: {e}")

    # 5. Run Amazon Pricing Module and save to database
    print(f"[{datetime.now()}] Running Amazon Pricing Module...")
    try:
        amazon_df = run_amazon_pricing()
        amazon_records = amazon_df.to_dicts()
        
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE amazon_pricing_results")
        
        import math
        def safe_float(val):
            try:
                v = float(val or 0)
                if math.isinf(v) or math.isnan(v):
                    return 0.0
                return v
            except:
                return 0.0

        def safe_int(val):
            try:
                return int(safe_float(val))
            except:
                return 0

        for row in amazon_records:
            cursor.execute("""
                INSERT INTO amazon_pricing_results 
                (master_sku, original_category, amazon_cat, remark, cost, mrp, uniware, fba, sjit, fbf, launch_date, loc, cost_into_percent, cost_after_percent, return_charge, gst_on_return, final_tp, required_selling_price, selected_price_range, selected_fixed_fee_range, commission_percent, commission_amount, fixed_closing_fee, fba_pick_pack, technology_fee, full_shipping_fee, whf_percent_on_shipping, shipping_fee_charged, total_charges, final_value_after_charges, old_daily_sp, old_deal_sp, sett_acc_panel, net_profit_on_sp, net_profit_percent_on_sp, net_profit_percent_on_tp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("Master_SKU"), row.get("Original_Category"), row.get("Amazon Cat"), row.get("Remark"),
                safe_float(row.get("Cost")), safe_float(row.get("MRP")), safe_int(row.get("Uniware", row.get("Uniware Stock"))), safe_int(row.get("FBA")), safe_int(row.get("Sjit")), safe_int(row.get("FBF")),
                row.get("Launch Date"), row.get("LOC"), safe_float(row.get("Cost into %")), safe_float(row.get("Cost after %")), safe_float(row.get("Return Charge")), safe_float(row.get("GST on Return")),
                safe_float(row.get("Final TP")), safe_float(row.get("Required Selling Price")), row.get("Selected Price Range"), row.get("Selected Fixed Fee Range"),
                safe_float(row.get("Commission %")), safe_float(row.get("Commission Amount")), safe_float(row.get("Fixed Closing Fee")), safe_float(row.get("FBA Pick Pack")), safe_float(row.get("Technology Fee")), safe_float(row.get("Full Shipping Fee")),
                safe_float(row.get("WHF % On Shipping")), safe_float(row.get("Shipping Fee Charged")), safe_float(row.get("Total Charges")), safe_float(row.get("Final Value After Charges")), safe_float(row.get("Old Daily SP")),
                safe_float(row.get("Old Deal SP")), safe_float(row.get("Sett Acc Panel")), safe_float(row.get("Net Profit On SP")), safe_float(row.get("Net Profit % On SP")), safe_float(row.get("Net Profit % On TP"))
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[{datetime.now()}] Amazon pricing complete! Saved {len(amazon_records)} records.")
    except Exception as e:
        print(f"[{datetime.now()}] Error updating amazon pricing database: {e}")

if __name__ == "__main__":
    run_pipeline()
