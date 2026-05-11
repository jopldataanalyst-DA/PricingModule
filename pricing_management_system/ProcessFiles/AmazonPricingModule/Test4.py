import polars as pl

df = pl.read_csv(r'D:\VatsalFiles\PricingModule\Data\CatalogData.csv')
print("df", df)

# df = df.unique()
df = df.unique(subset=["Master SKU"], keep="last")
print("df", df)

df.write_csv(r'D:\VatsalFiles\PricingModule\Data\CatalogData.csv')