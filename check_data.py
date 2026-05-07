import polars as pl

df = pl.read_csv("D:/VatsalFiles/PricingModule/Data/PricingModuleData.csv")
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns}")

for col in ["Uniware Stock", "FBA", "FBF", "SJIT"]:
    print(f"{col}: {df[col].sum()}")