import sys

sys.path.append(r"D:\VatsalFiles\PricingModule\pricing_management_system")
from ProcessFiles.DatabaseModule.AdvanceDatabase import MySqlDatabase

DB_Config = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module",
}

PricingDb = MySqlDatabase(DB_Config, PoolName="PricingPool")
print("Database connection established successfully.")
print(PricingDb.ShowTables())
# exit()


# Amazon Master SKU Code Mapping Dataframe
AMSCMdf = PricingDb.ReadTable("amazon_sku_code_mapping")
print(AMSCMdf.head())
print(AMSCMdf.height)


Limit = 15000

# B2B Amazon Sells Dataframe
B2BASdf = PricingDb.ReadLimit("amazon_sales_b2b", Limit)
B2BASdf = B2BASdf.select(
    [
        "id",
        "Seller_Gstin",
        "Invoice_Number",
        "Invoice_Date",
        "Transaction_Type",
        "Order_Id",
        "Order_Date",
        "Quantity",
        "Asin",
        "Hsn/sac",
        "Sku",
        "Ship_From_City",
        "Ship_From_State",
        "Ship_From_Postal_Code",
        "Ship_To_City",
        "Ship_To_State",
        "Ship_To_Postal_Code",
        "Invoice_Amount",
        "Tax_Exclusive_Gross",
        "Total_Tax_Amount",
        "Warehouse_Id",
        "Fulfillment_Channel",
    ]
)

print(B2BASdf.head())
print(B2BASdf.height)

df = B2BASdf.join(AMSCMdf, left_on="Sku", right_on="Master_SKU", how="left")
print(df.head())
print(df.height)


# Analytics Process


