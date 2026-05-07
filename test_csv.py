import polars as pl

# Read both CSV files directly
item_master = pl.read_csv("D:/VatsalFiles/PricingModule/Data/ItemMaster.csv")
selected = pl.read_csv("D:/VatsalFiles/PricingModule/Data/ItemMaster_selected.csv")

print("=== ItemMaster.csv ===")
print(f"Shape: {item_master.shape}")
u = item_master['Master SKU'].n_unique()
print(f"Unique SKUs: {u}")
print(f"Duplicates: {item_master.shape[0] - u}")

print("\n=== ItemMaster_selected.csv ===")
print(f"Shape: {selected.shape}")
u2 = selected['Master SKU'].n_unique()
print(f"Unique SKUs: {u2}")
print(f"Duplicates: {selected.shape[0] - u2}")

print("\n=== Direct stock totals from selected CSV ===")
print(f"Uniware Stock: {selected['Uniware Stock'].sum()}")
print(f"FBA: {selected['FBA'].sum()}")
print(f"FBF: {selected['FBF'].sum()}")
print(f"SJIT: {selected['SJIT'].sum()}")