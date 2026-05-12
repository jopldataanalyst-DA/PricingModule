"""CatalogData duplicate cleanup helper.

Use case:
    Reads Data/CatalogData.csv, keeps the last row for each Master SKU, and
    writes the cleaned file back. Run manually only when the catalog CSV needs
    deduplication.
"""

import polars as pl

df = pl.read_csv(r'D:\VatsalFiles\PricingModule\Data\CatalogData.csv')
print("df", df)

# df = df.unique()
df = df.unique(subset=["Master SKU"], keep="last")
print("df", df)

df.write_csv(r'D:\VatsalFiles\PricingModule\Data\CatalogData.csv')
