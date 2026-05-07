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
    df.columns = new_columns

    # Remove empty rows
    df = df.filter(pl.col("uni").is_not_null())

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

    # Save to CSV if output_path is provided
    if output_path:
        df.write_csv(output_path)

    return df

# Example usage:
df = clean_stock_data(
    url="https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv",
    output_path="Data/StockCleaned.csv"
)
print(df.head())