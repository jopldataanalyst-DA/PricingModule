"""Data refresh pipelines for inventory and Amazon pricing.

Use case:
    Pulls the latest stock data, rebuilds the Item Master dashboard table from
    item_master/catalog_pricing/stock_update, and runs the Amazon pricing model
    so the frontend can read fast, precomputed MySQL tables.
"""

import polars as pl
import requests
from io import BytesIO
from pathlib import Path
from datetime import datetime
from database import init_db, get_database
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
    """Download the stock Google Sheet CSV and normalize its raw columns."""
    response = requests.get(url)
    response.raise_for_status()
    raw_df = pl.read_csv(BytesIO(response.content), has_header=False)
    df = raw_df.slice(3)
    new_columns = ["uni", "uni_stock", "fba", "fba_stock", "fbf", "fbf_stock", "sjit", "sjit_stock"]
    if df.width < len(new_columns):
        raise ValueError(f"Stock sheet has {df.width} columns, expected at least {len(new_columns)}")
    df = df.select(df.columns[:len(new_columns)])
    df.columns = new_columns
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
    sku_columns = ["uni", "fba", "fbf", "sjit"]
    df = df.filter(
        pl.any_horizontal([
            pl.col(c).is_not_null() & (pl.col(c) != "")
            for c in sku_columns
        ])
    )
    return df

def run_inventory_pipeline():
    """Refresh stock_update and rebuild stock_items for the Item Master page."""
    init_db()
    print(f"[{datetime.now()}] Starting inventory pipeline upgrade...")
    
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
        """Aggregate one stock channel by Master SKU."""
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
        db = get_database()
        db.ReplaceTable(stock_merged, "stock_update")
        print(f"[{datetime.now()}] stock_update table updated with {stock_merged.height} records.")
    except Exception as e:
        print(f"[{datetime.now()}] Error saving to stock_update: {e}")
        return

    # 3. Fetch stock_items, stock_update, and catalog_pricing from Database for final dashboard
    print(f"[{datetime.now()}] Generating final dashboard from database tables...")
    try:
        db = get_database()
        
        # Fetch item_master as the base for the dashboard
        item_master_rows = db.FetchAll("SELECT * FROM item_master")
        if not item_master_rows:
            print(f"[{datetime.now()}] item_master table is empty – skipping dashboard rebuild.")
        else:
            item_master_df = pl.from_dicts(item_master_rows)
            rename_map = {
                "Master SKU": "sku_code",
                "Style ID / Parent SKU": "item_name",
                "Size": "size",
                "Category": "category",
                "Loc": "location",
                "Child Remark": "child_remark",
                "Parent Remark": "parent_remark",
                "Type": "item_type"
            }
            rename_dict = {k: v for k, v in rename_map.items() if k in item_master_df.columns}
            stock_items_df = item_master_df.rename(rename_dict).with_columns([
                pl.col("sku_code").cast(pl.Utf8).str.strip_chars().str.to_uppercase(),
                pl.col("item_name").cast(pl.Utf8).fill_null(""),
                pl.col("size").cast(pl.Utf8).fill_null(""),
                pl.col("category").cast(pl.Utf8).fill_null(""),
                pl.col("location").cast(pl.Utf8).fill_null(""),
                pl.col("child_remark").cast(pl.Utf8).fill_null(""),
                pl.col("parent_remark").cast(pl.Utf8).fill_null(""),
                pl.col("item_type").cast(pl.Utf8).fill_null(""),
                pl.lit("").alias("catalog"),
                pl.lit(0.0).alias("cost"),
                pl.lit(0.0).alias("price"),
                pl.lit(0.0).alias("mrp"),
                pl.lit(0.0).alias("up_price"),
                pl.lit(0).alias("available_atp"),
                pl.lit(0).alias("fba_stock"),
                pl.lit(0).alias("fbf_stock"),
                pl.lit(0).alias("sjit_stock"),
                pl.lit("").alias("updated")
            ])
            if "id" in stock_items_df.columns:
                stock_items_df = stock_items_df.drop("id")

            # Fetch stock_update
            stock_update_rows = db.FetchAll("SELECT * FROM stock_update")
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
            catalog_pricing_rows = db.FetchAll("SELECT * FROM catalog_pricing")
            if catalog_pricing_rows:
                catalog_pricing_df = pl.from_dicts(catalog_pricing_rows)
                rename_map = {
                    "Master SKU": "master_sku",
                    "Launch Date": "launch_date", 
                    "Catalog Name": "catalog_name",
                    "Cost": "cost",
                    "Wholesale Price": "wholesale_price",
                    "Up Price": "up_price",
                    "MRP": "mrp"
                }
                rename_dict = {k: v for k, v in rename_map.items() if k in catalog_pricing_df.columns}
                catalog_pricing_df = catalog_pricing_df.rename(rename_dict).with_columns([
                    pl.col("master_sku").cast(pl.Utf8).str.strip_chars().str.to_uppercase(),
                    pl.col("cost").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("mrp").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("launch_date").cast(pl.Utf8).fill_null(""),
                    pl.col("catalog_name").cast(pl.Utf8).fill_null("")
                ]).unique(subset=["master_sku"], keep="last")
            else:
                catalog_pricing_df = pl.DataFrame(schema={
                    "master_sku": pl.String, "launch_date": pl.String, "catalog_name": pl.String,
                    "cost": pl.Float64, "wholesale_price": pl.Float64, "up_price": pl.Float64,
                    "mrp": pl.Float64
                })

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

            # Merge catalog pricing (cost / mrp / price / up_price / catalog / launch_date)
            stock_items_df = stock_items_df.join(
                catalog_pricing_df.select([
                    "master_sku", "cost", "mrp", "wholesale_price", "up_price", "catalog_name", "launch_date"
                ]).rename({
                    "master_sku": "sku_code",
                    "cost": "_cat_cost",
                    "mrp": "_cat_mrp",
                    "wholesale_price": "_cat_price",
                    "up_price": "_cat_up_price",
                    "catalog_name": "_cat_catalog",
                    "launch_date": "_cat_launch",
                }),
                on="sku_code",
                how="left"
            ).with_columns([
                pl.coalesce(["_cat_cost", "cost"]).fill_null(0.0).alias("cost"),
                pl.coalesce(["_cat_mrp",  "mrp"]).fill_null(0.0).alias("mrp"),
                pl.coalesce(["_cat_price", "price"]).fill_null(0.0).alias("price"),
                pl.coalesce(["_cat_up_price", "up_price"]).fill_null(0.0).alias("up_price"),
                pl.coalesce(["_cat_catalog", "catalog"]).fill_null("").alias("catalog"),
                pl.coalesce(["_cat_launch", "updated"]).fill_null("").alias("updated"),
            ]).drop(["_cat_cost", "_cat_mrp", "_cat_price", "_cat_up_price", "_cat_catalog", "_cat_launch"])

            # Persist updated values back into stock_items
            try:
                stock_items_df = stock_items_df.select([
                    "sku_code", "item_name", "size", "category", "location",
                    "child_remark", "parent_remark", "item_type", "catalog",
                    "cost", "price", "mrp", "up_price", "available_atp",
                    "fba_stock", "fbf_stock", "sjit_stock", "updated"
                ])
                db.ReplaceTable(stock_items_df, "stock_items")
                print(f"[{datetime.now()}] stock_items refreshed with {stock_items_df.height} records.")
            except Exception as e:
                print(f"[{datetime.now()}] Error refreshing stock_items: {e}")
                return

    except Exception as e:
        print(f"[{datetime.now()}] Error in dashboard rebuild step: {e}")

def run_amazon_pipeline():
    """Run the Amazon pricing workbook logic and persist the result table."""
    init_db()
    # 5. Run Amazon Pricing Module and save to database
    print(f"[{datetime.now()}] Running Amazon Pricing Module...")
    try:
        amazon_df = run_amazon_pricing()
        amazon_records = amazon_df.to_dicts()

        import math
        def safe_float(val):
            """Convert workbook values to finite floats for MySQL."""
            try:
                v = float(val or 0)
                if math.isinf(v) or math.isnan(v):
                    return 0.0
                return v
            except:
                return 0.0

        def safe_int(val):
            """Convert workbook values to ints through the same float cleanup."""
            try:
                return int(safe_float(val))
            except:
                return 0

        result_rows = []
        for row in amazon_records:
            result_rows.append({
                "master_sku": row.get("Master_SKU"),
                "original_category": row.get("Original_Category"),
                "amazon_cat": row.get("Amazon Cat"),
                "remark": row.get("Remark"),
                "cost": safe_float(row.get("Cost")),
                "mrp": safe_float(row.get("MRP")),
                "uniware": safe_int(row.get("Uniware", row.get("Uniware Stock"))),
                "fba": safe_int(row.get("FBA")),
                "sjit": safe_int(row.get("Sjit")),
                "fbf": safe_int(row.get("FBF")),
                "launch_date": row.get("Launch Date"),
                "loc": row.get("LOC"),
                "cost_into_percent": safe_float(row.get("Cost into %")),
                "cost_after_percent": safe_float(row.get("Cost after %")),
                "return_charge": safe_float(row.get("Return Charge")),
                "gst_on_return": safe_float(row.get("GST on Return")),
                "final_tp": safe_float(row.get("Final TP")),
                "required_selling_price": safe_float(row.get("Required Selling Price")),
                "selected_price_range": row.get("Selected Price Range"),
                "selected_fixed_fee_range": row.get("Selected Fixed Fee Range"),
                "commission_percent": safe_float(row.get("Commission %")),
                "commission_amount": safe_float(row.get("Commission Amount")),
                "fixed_closing_fee": safe_float(row.get("Fixed Closing Fee")),
                "fba_pick_pack": safe_float(row.get("FBA Pick Pack")),
                "technology_fee": safe_float(row.get("Technology Fee")),
                "full_shipping_fee": safe_float(row.get("Full Shipping Fee")),
                "whf_percent_on_shipping": safe_float(row.get("WHF % On Shipping")),
                "shipping_fee_charged": safe_float(row.get("Shipping Fee Charged")),
                "total_charges": safe_float(row.get("Total Charges")),
                "final_value_after_charges": safe_float(row.get("Final Value After Charges")),
                "old_daily_sp": safe_float(row.get("Old Daily SP")),
                "old_deal_sp": safe_float(row.get("Old Deal SP")),
                "sett_acc_panel": safe_float(row.get("Sett Acc Panel")),
                "net_profit_on_sp": safe_float(row.get("Net Profit On SP")),
                "net_profit_percent_on_sp": safe_float(row.get("Net Profit % On SP")),
                "net_profit_percent_on_tp": safe_float(row.get("Net Profit % On TP")),
            })

        get_database().ReplaceTable(pl.DataFrame(result_rows), "amazon_pricing_results")
        print(f"[{datetime.now()}] Amazon pricing complete! Saved {len(amazon_records)} records.")
    except Exception as e:
        print(f"[{datetime.now()}] Error updating amazon pricing database: {e}")

def run_pipeline():
    """Run both dashboard refresh steps in the required order."""
    run_inventory_pipeline()
    run_amazon_pipeline()

if __name__ == "__main__":
    run_pipeline()
