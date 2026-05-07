import requests
import polars as pl
from io import BytesIO

STOCK_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"

print("Fetching stock data...")
response = requests.get(STOCK_URL)
response.raise_for_status()
raw_df = pl.read_csv(BytesIO(response.content), has_header=False)
print(f"Raw shape: {raw_df.shape}")

# Skip header rows
df = raw_df.slice(3)
new_columns = ["uni", "uni_stock", "fba", "fba_stock", "fbf", "fbf_stock", "sjit", "sjit_stock"]
df.columns = new_columns
df = df.filter(pl.col("uni").is_not_null())
print(f"After skip/shape: {df.shape}")

# Convert stock columns to numbers
for col in ["uni_stock", "fba_stock", "fbf_stock", "sjit_stock"]:
    df = df.with_columns(pl.col(col).str.replace(",", "").cast(pl.Int64, strict=True).fill_null(0))

print(f"\nStock totals from Google Sheet:")
print(f"  Uniware Stock: {df['uni_stock'].sum()}")
print(f"  FBA: {df['fba_stock'].sum()}")
print(f"  FBF: {df['fbf_stock'].sum()}")
print(f"  SJIT: {df['sjit_stock'].sum()}")

# Check ItemMaster_selected.csv
print("\n--- ItemMaster_selected.csv ---")
item_csv = pl.read_csv("D:/VatsalFiles/PricingModule/Data/ItemMaster_selected.csv")
print(f"Shape: {item_csv.shape}")
print(f"Columns: {item_csv.columns}")

# Check stock columns
for col in ["Uniware Stock", "FBA", "FBF", "SJIT"]:
    if col in item_csv.columns:
        print(f"{col}: {item_csv[col].sum()}")
    else:
        print(f"{col}: NOT FOUND")