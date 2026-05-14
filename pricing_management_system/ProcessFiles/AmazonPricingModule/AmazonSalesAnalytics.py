import sys
import polars as pl

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

# Amazon Master SKU Code Mapping Dataframe
AMSCMdf = PricingDb.ReadTable("amazon_sku_code_mapping")
Limit = 15000

# B2B Amazon Sells Dataframe
B2BASdf = PricingDb.ReadLimit("amazon_sales_b2b", Limit)
B2BASdf = B2BASdf.select([
    "id", "Seller_Gstin", "Invoice_Number", "Invoice_Date", "Transaction_Type", 
    "Order_Id", "Order_Date", "Quantity", "Asin", "Hsn/sac", "Sku", 
    "Ship_From_City", "Ship_From_State", "Ship_From_Postal_Code", 
    "Ship_To_City", "Ship_To_State", "Ship_To_Postal_Code", 
    "Invoice_Amount", "Tax_Exclusive_Gross", "Total_Tax_Amount", 
    "Warehouse_Id", "Fulfillment_Channel"
])

df = B2BASdf.join(AMSCMdf, left_on="Sku", right_on="Master_SKU", how="left")

# --- DATA PREPARATION ---
df = df.with_columns([
    pl.col("Invoice_Amount").cast(pl.Float64),
    pl.col("Total_Tax_Amount").cast(pl.Float64),
    pl.col("Quantity").cast(pl.Float64),
    pl.col("Invoice_Date").dt.truncate("1d").alias("Date_Day"),
    pl.col("Invoice_Date").dt.truncate("1mo").alias("Date_Month"),
    pl.col("Invoice_Date").dt.truncate("1w").alias("Date_Week"),
])

def print_section(title):
    print("\n" + "="*60)
    print(f" {title} ".center(60, "="))
    print("="*60)

# ==============================================================================
# 1. SALES QUESTIONS (1-9)
# ==============================================================================
print_section("1. SALES PERFORMANCE")
total_revenue = df.select(pl.col("Invoice_Amount").sum()).item()
total_orders = df.select(pl.col("Order_Id").n_unique()).item()
total_units = df.select(pl.col("Quantity").sum()).item()

print(f"Q1: Total Revenue: ₹{total_revenue:,.2f}")
print(f"Q2: Total Orders: {total_orders:,}")
print(f"Q3: Total Units Sold: {total_units:,}")
print(f"Q4: Average Order Value (AOV): ₹{(total_revenue / total_orders if total_orders > 0 else 0):,.2f}")
print(f"Q5: Avg Selling Price per Unit: ₹{(total_revenue / total_units if total_units > 0 else 0):,.2f}")

print("\nQ6: Revenue Trends (Daily, Weekly, Monthly):")
# Daily Trend (Top 5)
print("- Monthly Revenue:")
monthly_rev = df.group_by("Date_Month").agg(pl.col("Invoice_Amount").sum().alias("Revenue")).sort("Date_Month")
print(monthly_rev)

print("\nQ7-8: Peak Performance periods:")
best_day = df.group_by("Date_Day").agg(pl.col("Invoice_Amount").sum().alias("Rev")).sort("Rev", descending=True).head(1)
best_month = monthly_rev.sort("Revenue", descending=True).head(1)
print(f"- Best Day: {best_day['Date_Day'][0]} (₹{best_day['Rev'][0]:,.2f})")
print(f"- Best Month: {best_month['Date_Month'][0]} (₹{best_month['Revenue'][0]:,.2f})")

# Q9: Sales Growth (MoM)
if monthly_rev.height > 1:
    growth = monthly_rev.with_columns(
        (pl.col("Revenue").diff() / pl.col("Revenue").shift(1) * 100).alias("Growth_%")
    )
    print("\nQ9: Month-over-Month Growth:")
    print(growth)

# ==============================================================================
# 2. TRANSACTION QUESTIONS (10-15)
# ==============================================================================
print_section("2. TRANSACTION ANALYSIS")
tx_stats = df.group_by("Transaction_Type").agg([
    pl.count("Order_Id").alias("Orders"),
    (pl.count("Order_Id") / df.height * 100).alias("Percentage_%"),
    pl.col("Invoice_Amount").sum().alias("Revenue_Impact")
])
print("Q10: Distribution of Transaction Types:")
print(tx_stats)

refund_count = df.filter(pl.col("Transaction_Type").str.contains("Refund")).select(pl.col("Order_Id").n_unique()).item()
cancel_count = df.filter(pl.col("Transaction_Type").str.contains("Cancel")).select(pl.col("Order_Id").n_unique()).item()
print(f"\nQ11: Total Refunded Orders: {refund_count}")
print(f"Q12: Total Cancelled Orders: {cancel_count}")
print(f"Q13: Refund Rate: {(refund_count / total_orders * 100 if total_orders > 0 else 0):.2f}%")
print("Q14: Highest Revenue Loss Source:")
print(tx_stats.filter(pl.col("Revenue_Impact") < 0).sort("Revenue_Impact"))

# ==============================================================================
# 3. PRODUCT QUESTIONS (16-23)
# ==============================================================================
print_section("3. PRODUCT INSIGHTS")
prod_stats = df.group_by("Sku").agg([
    pl.col("Invoice_Amount").sum().alias("Revenue"),
    pl.col("Quantity").sum().alias("Units"),
    pl.col("Total_Tax_Amount").sum().alias("Tax")
]).sort("Revenue", descending=True)

print("Q16-17: Top 5 SKUs by Revenue & Units:")
print(prod_stats.select(["Sku", "Revenue", "Units"]).head(5))

print("\nQ18: Top 5 ASINs:")
print(df.group_by("Asin").agg(pl.col("Invoice_Amount").sum().alias("Revenue")).sort("Revenue", descending=True).head(5))

print("\nQ19: Slow-moving Products (Bottom 5 by Units):")
print(prod_stats.filter(pl.col("Units") > 0).sort("Units").head(5))

print("\nQ20: Top 5 Products with High Refunds:")
refund_prods = df.filter(pl.col("Transaction_Type").str.contains("Refund")).group_by("Sku").agg(pl.count("Order_Id").alias("Refund_Count")).sort("Refund_Count", descending=True)
print(refund_prods.head(5))

# Q22: Pareto Analysis (80% Sales)
prod_stats = prod_stats.with_columns([
    (pl.col("Revenue").cum_sum() / total_revenue).alias("Cumulative_Share")
])
pareto_skus = prod_stats.filter(pl.col("Cumulative_Share") <= 0.8)
print(f"\nQ22: {pareto_skus.height} SKUs contribute 80% of total sales.")

print("\nQ23: Top HSN Categories:")
print(df.group_by("Hsn/sac").agg(pl.col("Invoice_Amount").sum().alias("Revenue")).sort("Revenue", descending=True).head(5))

# ==============================================================================
# 4. CUSTOMER GEOGRAPHY (24-30)
# ==============================================================================
print_section("4. GEOGRAPHY ANALYSIS")
geo_stats = df.group_by("Ship_To_State").agg([
    pl.col("Order_Id").n_unique().alias("Orders"),
    pl.col("Invoice_Amount").sum().alias("Revenue"),
    pl.col("Quantity").sum().alias("Units")
]).sort("Orders", descending=True)

print("Q24 & Q26: Top 5 States by Orders and Units:")
print(geo_stats.select(["Ship_To_State", "Orders", "Units"]).head(5))

print("\nQ25: Top 5 Cities by Revenue:")
print(df.group_by("Ship_To_City").agg(pl.col("Invoice_Amount").sum().alias("Revenue")).sort("Revenue", descending=True).head(5))

print("\nQ28: Top 5 Postal Codes:")
print(df.group_by("Ship_To_Postal_Code").agg(pl.col("Invoice_Amount").sum().alias("Revenue")).sort("Revenue", descending=True).head(5))

# ==============================================================================
# 5. SHIPPING & ROUTES (31-35)
# ==============================================================================
print_section("5. SHIPPING LOGISTICS")
print("Q31-33: Origin Locations (Top 5 States):")
print(df.group_by("Ship_From_State").agg(pl.count("Order_Id").alias("Shipments")).sort("Shipments", descending=True).head(5))

print("\nQ34: Common Shipping Routes (State to State):")
routes = df.group_by(["Ship_From_State", "Ship_To_State"]).agg(pl.count("Order_Id").alias("Route_Volume")).sort("Route_Volume", descending=True)
print(routes.head(5))

# ==============================================================================
# 6. WAREHOUSE & FULFILLMENT (36-45)
# ==============================================================================
print_section("6. WAREHOUSE & FULFILLMENT")
wh_stats = df.group_by("Warehouse_Id").agg([
    pl.col("Order_Id").n_unique().alias("Orders"),
    pl.col("Invoice_Amount").sum().alias("Revenue")
]).sort("Orders", descending=True)
print("Q36-37: Warehouse Performance:")
print(wh_stats)

print("\nQ41-44: Fulfillment Performance:")
fc_stats = df.group_by("Fulfillment_Channel").agg([
    pl.col("Order_Id").n_unique().alias("Orders"),
    pl.col("Invoice_Amount").sum().alias("Revenue"),
    pl.col("Quantity").sum().alias("Volume")
]).sort("Revenue", descending=True)
print(fc_stats)

# ==============================================================================
# 7. TAX QUESTIONS (46-50)
# ==============================================================================
print_section("7. TAX ANALYSIS")
total_tax = df.select(pl.col("Total_Tax_Amount").sum()).item()
print(f"Q46: Total Tax Collected: ₹{total_tax:,.2f}")
print(f"Q47: Effective Tax Rate: {(total_tax / total_revenue * 100 if total_revenue > 0 else 0):.2f}%")

print("\nQ48: Top 5 States generating highest Tax:")
print(df.group_by("Ship_To_State").agg(pl.col("Total_Tax_Amount").sum().alias("Tax")).sort("Tax", descending=True).head(5))

# ==============================================================================
# 8. QUALITY & OPERATIONAL (51-60)
# ==============================================================================
print_section("8. QUALITY & OPERATIONS")
print(f"Q52: Average Quantity per Order: {(total_units / total_orders if total_orders > 0 else 0):.2f}")

# Duplicate Checks
dup_inv = df.select("Invoice_Number").height - df.select("Invoice_Number").n_unique()
dup_ord = df.select("Order_Id").height - df.select("Order_Id").n_unique()
print(f"Q53: Rows with Duplicate Invoice Numbers: {dup_inv}")
print(f"Q54: Rows with Duplicate Order IDs: {dup_ord} (Common for multi-item orders)")

# Q56: Restock Suggestions (Top velocity SKUs)
print("\nQ56: SKUs suggested for Restock (High Velocity):")
print(prod_stats.select(["Sku", "Units"]).head(5))

# ==============================================================================
# 9. EXECUTIVE SUMMARY (61-67)
# ==============================================================================
print_section("9. EXECUTIVE SUMMARY")
print("Q61: Top Business Driver (Highest Revenue SKU):", prod_stats['Sku'][0])
print(f"Q62: Revenue Leakage (Refunds/Cancellations Impact): ₹{tx_stats.filter(pl.col('Transaction_Type').is_in(['Refund', 'Cancel', 'EInvoiceCancel'])).select(pl.col('Revenue_Impact').sum()).item():,.2f}")
print(f"Q64: Recommended Inventory Focus: Top 5 Pareto SKUs")
print(f"Q67: Biggest Risk: Refund Rate is {(refund_count / total_orders * 100 if total_orders > 0 else 0):.2f}%")

print("\n" + "="*60)
print(" REPORT COMPLETE ".center(60, "="))
print("="*60)
