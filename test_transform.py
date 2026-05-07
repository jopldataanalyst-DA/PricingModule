import polars as pl
import requests
from io import BytesIO

url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"

# Step 1: clean_stock_data
response = requests.get(url)
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

print("After clean_stock_data:")
print(f"  Uni: {df['uni_stock'].sum()}, FBA: {df['fba_stock'].sum()}, FBF: {df['fbf_stock'].sum()}, SJIT: {df['sjit_stock'].sum()}")

# Step 2: Column rename
stock = df
stock.columns = ["Uniware SKU", "Uniware Stock", "FBA SKU", "FBA", "FBF SKU", "FBF", "SJIT SKU", "SJIT"]
stock = stock.with_columns([
    pl.col(c).cast(pl.Utf8).str.strip_chars()
    for c in ["Uniware SKU", "FBA SKU", "FBF SKU", "SJIT SKU"]
] + [
    pl.col(c).cast(pl.Int64, strict=False).fill_null(0)
    for c in ["Uniware Stock", "FBA", "FBF", "SJIT"]
])

print("\nAfter column rename:")
print(f"  Uni: {stock['Uniware Stock'].sum()}, FBA: {stock['FBA'].sum()}, FBF: {stock['FBF'].sum()}, SJIT: {stock['SJIT'].sum()}")

# Step 3: make_stock_df
def make_stock_df(sku_col, stock_col):
    return (
        stock
        .select([
            pl.col(sku_col).alias("Master SKU"),
            pl.col(stock_col)
        ])
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

for name, tbl in zip(["Uni", "FBA", "FBF", "SJIT"], stock_tables):
    print(f"  {name} table: {tbl.shape[0]} SKUs, total {tbl.columns[1]}: {tbl[tbl.columns[1]].sum()}")

# Step 4: Join
item = pl.read_csv("D:/VatsalFiles/PricingModule/Data/ItemMaster.csv", ignore_errors=True)
final = item.with_columns(
    pl.col("Master SKU").cast(pl.Utf8).str.strip_chars()
).rename({"Loc": "Location"})

print(f"\nItemMaster: {final.shape[0]} rows")

for table in stock_tables:
    final = final.join(table, on="Master SKU", how="left")

print(f"After join: {final.shape[0]} rows")

# Step 5: Final select
final = final.with_columns([
    pl.lit(0.0).alias("Cost"),
    pl.lit(0.0).alias("Wholesale Price"),
    pl.lit("").alias("Catalog Name"),
    pl.lit(0.0).alias("Up Price"),
    pl.lit("").alias("Launch Date"),
    *[
        pl.col(c).fill_null(0)
        for c in ["Uniware Stock", "FBA", "FBF", "SJIT"]
    ]
]).select([
    "Launch Date",
    "Master SKU",
    "Style ID / Parent SKU",
    "Size",
    "Category",
    "Location",
    "Catalog Name",
    "Cost",
    "Wholesale Price",
    "Up Price",
    "Uniware Stock",
    "FBA",
    "FBF",
    "SJIT",
])

print("\n=== FINAL OUTPUT ===")
print(f"Shape: {final.shape}")
print(f"Uniware: {final['Uniware Stock'].sum()}")
print(f"FBA: {final['FBA'].sum()}")
print(f"FBF: {final['FBF'].sum()}")
print(f"SJIT: {final['SJIT'].sum()}")