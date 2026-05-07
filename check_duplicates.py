import polars as pl

# Check duplicate handling
item = pl.read_csv("D:/VatsalFiles/PricingModule/Data/ItemMaster.csv")
print(f"ItemMaster: {item.shape[0]} rows, {item['Master SKU'].n_unique()} unique")

# Check for duplicates
dups = item.group_by("Master SKU").agg(pl.len().alias("cnt")).filter(pl.col("cnt") > 1)
print(f"Duplicate SKUs: {dups.shape[0]}")

# Show some duplicates
if dups.shape[0] > 0:
    print("\nSample duplicates:")
    for sku in dups['Master SKU'].head(3).to_list():
        rows = item.filter(pl.col("Master SKU") == sku)
        print(f"  {sku}: {rows.shape[0]} rows")
        for r in rows.rows():
            print(f"    {r}")