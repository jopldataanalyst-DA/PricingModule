import requests
import polars as pl
from io import BytesIO
import sys
sys.path.insert(0, "D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system")

# Trace what get_processed_data does step by step
STOCK_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"

# Make stock df function from items.py
def make_stock_df(stock_raw, stock_col, qty_col):
    return (
        stock_raw.select([
            pl.col(stock_col).str.strip_chars().alias("Master SKU"),
            pl.col(qty_col).str.replace(",", "").str.strip_chars().cast(pl.Int64, strict=False).fill_null(0).alias(qty_col)
        ])
        .filter(pl.col("Master SKU").is_not_null())
    )

# Load stock
response = requests.get(STOCK_URL)
stock_raw = pl.read_csv(BytesIO(response.content), has_header=False)
stock_raw.columns = ["Uniware SKU", "Uniware Stock", "FBA SKU", "FBA", "FBF SKU", "FBF", "SJIT SKU", "SJIT"]
stock_raw = stock_raw.slice(3)

stock_tables = [
    make_stock_df(stock_raw, "Uniware SKU", "Uniware Stock"),
    make_stock_df(stock_raw, "FBA SKU", "FBA"),
    make_stock_df(stock_raw, "FBF SKU", "FBF"),
    make_stock_df(stock_raw, "SJIT SKU", "SJIT"),
]

for name, tbl in zip(["Uni", "FBA", "FBF", "SJIT"], stock_tables):
    print(f"{name}: {tbl.shape[0]} rows, total {tbl.columns[1]}: {tbl[tbl.columns[1]].sum()}")

# Load ItemMaster
ITEM_MASTER_CSV = "D:/VatsalFiles/PricingModule/Data/ItemMaster.csv"
item_master = pl.read_csv(ITEM_MASTER_CSV, ignore_errors=True)
final = item_master.with_columns(
    pl.col("Master SKU").cast(pl.Utf8).str.strip_chars()
).rename({"Loc": "Location"})

print(f"\nItemMaster shape: {final.shape}")
print(f"Unique: {final['Master SKU'].n_unique()}")

# Join with stock
for tbl in stock_tables:
    final = final.join(tbl, on="Master SKU", how="left")

print(f"After join: {final.shape}")
print(f"Unique after join: {final['Master SKU'].n_unique()}")

# Check duplicates that cause inflated stock
dups = final.group_by("Master SKU").agg(pl.len().alias("cnt"))
multi_row = dups.filter(pl.col("cnt") > 1)
print(f"\nSKUs with multiple rows after join: {multi_row.shape[0]}")

# Show one duplicate example
if multi_row.shape[0] > 0:
    example_sku = multi_row['Master SKU'].head(1).to_list()[0]
    example = final.filter(pl.col("Master SKU") == example_sku)
    print(f"\nExample duplicate SKU: {example_sku}")
    print(f"Rows: {example.shape[0]}")
    print(example.select(["Master SKU", "Uniware Stock", "Cost"]))