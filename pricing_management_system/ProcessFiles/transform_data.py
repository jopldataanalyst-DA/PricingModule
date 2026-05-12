"""Legacy stock transformation prototype.

Use case:
    Downloads stock data from the Google Sheet, creates per-channel stock
    aggregates, joins them to ItemMaster/Catalog CSVs, and writes local CSV
    outputs. The production version of this logic now lives in data_pipeline.py.
"""

import polars as pl
import requests
from io import BytesIO

def clean_stock_data(url: str, output_path: str = None) -> pl.DataFrame:
    """
    Downloads, cleans, and optionally saves stock data from a Google Sheets CSV export.

    Args:
        url (str): Google Sheets CSV export URL.
        output_path (str, optional): Path to save the cleaned CSV. If None, does not save.

    Returns:
        pl.DataFrame: Cleaned Polars DataFrame.
    """
    # Download CSV
    response = requests.get(url)
    response.raise_for_status()

    # Read raw CSV (no header)
    raw_df = pl.read_csv(BytesIO(response.content), has_header=False)

    # Skip unwanted rows
    df = raw_df.slice(3)

    # Rename columns dynamically
    new_columns = [
        "uni", "uni_stock",
        "fba", "fba_stock",
        "fbf", "fbf_stock",
        "sjit", "sjit_stock"
    ]
    if df.width < len(new_columns):
        raise ValueError(f"Stock sheet has {df.width} columns, expected at least {len(new_columns)}")
    df = df.select(df.columns[:len(new_columns)])
    df.columns = new_columns

    # Clean and cast columns
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
    df = df.filter(
        pl.any_horizontal([
            pl.col(c).is_not_null() & (pl.col(c) != "")
            for c in ["uni", "fba", "fbf", "sjit"]
        ])
    )

    # Save to CSV if output_path is provided
    if output_path:
        df.write_csv(output_path)

    return df

base = r"D:\VatsalFiles\PricingModule\Data"
url = r"https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"


# Read files
stock = clean_stock_data(
    url=url,
    output_path=f"{base}/StockCleaned.csv"
)
item = pl.read_csv(f"{base}/ItemMaster.csv", ignore_errors=True)


# Rename stock columns as per new CSV
stock.columns = [
    "Uniware SKU", "Uniware Stock",
    "FBA SKU", "FBA",
    "FBF SKU", "FBF",
    "SJIT SKU", "SJIT"
]

# Clean stock
stock = stock.with_columns([
    pl.col(c).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    for c in ["Uniware SKU", "FBA SKU", "FBF SKU", "SJIT SKU"]
] + [
    pl.col(c).cast(pl.Int64, strict=False).fill_null(0)
    for c in ["Uniware Stock", "FBA", "FBF", "SJIT"]
])

# Function to create stock table
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

# Clean item master
final = item.with_columns(
    pl.col("Master SKU").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
).rename({"Loc": "Location"})

# Join all stock tables
for table in stock_tables:
    final = final.join(table, on="Master SKU", how="left")

# Final output
final = final.with_columns([
    pl.lit(0.0).alias("Cost"),
    pl.lit(0.0).alias("Wholesale Price"),
    pl.lit("").alias("Catalog Name"),
    pl.lit(0.0).alias("Up Price"),
    pl.lit(0.0).alias("MRP"),
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
    "MRP",
    "Uniware Stock",
    "FBA",
    "FBF",
    "SJIT",
])

print(final)

final.write_csv(f"{base}\\PricingModuleData.csv")
