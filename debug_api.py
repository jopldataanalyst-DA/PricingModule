import sys
sys.path.insert(0, "D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system")

from items import get_processed_data, global_cache

# Fresh load every time
global_cache.data = None
global_cache.last_updated = 0

df = get_processed_data()
print(f"Shape: {df.shape}")
print(f"Unique: {df['sku_code'].n_unique()}")
print("\n=== KPI after processing ===")
print(f"Uniware: {df['available_atp'].sum()}")
print(f"FBA: {df['fba_stock'].sum()}")
print(f"FBF: {df['fbf_stock'].sum()}")
print(f"SJIT: {df['sjit_stock'].sum()}")

# Now compare with load_items_csv
from items import load_items_csv
items = load_items_csv()
print("\n=== KPI from load_items_csv ===")
print(f"Uniware: {sum(x.get('available_atp',0) for x in items.values())}")
print(f"FBA: {sum(x.get('fba_stock',0) for x in items.values())}")
print(f"FBF: {sum(x.get('fbf_stock',0) for x in items.values())}")
print(f"SJIT: {sum(x.get('sjit_stock',0) for x in items.values())}")