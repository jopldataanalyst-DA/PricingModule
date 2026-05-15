"""
Amazon B2B Seller Analytics Report
====================================
Answers 95 business questions across 11 dimensions using Polars.
Covers: Revenue, Transactions, Products, Geography, Shipping,
        Warehouses, Fulfillment, Tax, Data Quality, Operations, Executive Summary.
"""

import sys
import polars as pl
from datetime import datetime

sys.path.append(r"D:\VatsalFiles\PricingModule\pricing_management_system")
from ProcessFiles.DatabaseModule.AdvanceDatabase import MySqlDatabase

# ── Config ──────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module",
}
LIMIT = 15_000
REFUND_TYPES   = ["Refund", "EInvoiceCancel"]
CANCEL_TYPES   = ["Cancel"]
SHIPMENT_TYPES = ["Shipment"]

# ── Helpers ──────────────────────────────────────────────────────────────────
def header(title: str, width: int = 70) -> None:
    print(f"\n{'═' * width}")
    print(f"  {title}")
    print(f"{'═' * width}")

def subheader(q: str) -> None:
    print(f"\n▸ {q}")

def fmt_inr(value: float) -> str:
    return f"₹{value:,.2f}"

def fmt_pct(value: float) -> str:
    return f"{value:.2f}%"

def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    return numerator / denominator if denominator else default

# ── Database ─────────────────────────────────────────────────────────────────
db = MySqlDatabase(DB_CONFIG, PoolName="PricingPool")
print("✓ Database connected.")

sku_map_df = db.ReadTable("amazon_sku_code_mapping")

raw_df = db.ReadLimit("amazon_sales_b2b", LIMIT).select([
    "id", "Seller_Gstin", "Invoice_Number", "Invoice_Date", "Transaction_Type",
    "Order_Id", "Order_Date", "Quantity", "Asin", "Hsn/sac", "Sku",
    "Ship_From_City", "Ship_From_State", "Ship_From_Postal_Code",
    "Ship_To_City", "Ship_To_State", "Ship_To_Postal_Code",
    "Invoice_Amount", "Tax_Exclusive_Gross", "Total_Tax_Amount",
    "Warehouse_Id", "Fulfillment_Channel",
])

# ── Join & Cast ───────────────────────────────────────────────────────────────
df = (
    raw_df
    .join(sku_map_df, left_on="Sku", right_on="Master_SKU", how="left")
    .with_columns([
        pl.col("Invoice_Amount").cast(pl.Float64),
        pl.col("Tax_Exclusive_Gross").cast(pl.Float64),
        pl.col("Total_Tax_Amount").cast(pl.Float64),
        pl.col("Quantity").cast(pl.Float64),
        pl.col("Invoice_Date").dt.truncate("1d").alias("Date_Day"),
        pl.col("Invoice_Date").dt.truncate("1w").alias("Date_Week"),
        pl.col("Invoice_Date").dt.truncate("1mo").alias("Date_Month"),
        # Intrastate flag: Ship_From_State == Ship_To_State
        (pl.col("Ship_From_State").str.to_uppercase() == pl.col("Ship_To_State").str.to_uppercase())
        .alias("Is_Intrastate"),
    ])
)

# Pre-filtered views (reused throughout)
shipments_df  = df.filter(pl.col("Transaction_Type").is_in(SHIPMENT_TYPES))
refunds_df    = df.filter(pl.col("Transaction_Type").is_in(REFUND_TYPES))
cancels_df    = df.filter(pl.col("Transaction_Type").is_in(CANCEL_TYPES))

gross_revenue  = shipments_df["Invoice_Amount"].sum()
refund_amount  = refunds_df["Invoice_Amount"].sum()   # already negative
net_revenue    = gross_revenue + refund_amount         # refund_amount is negative
total_orders   = df["Order_Id"].n_unique()
shipped_orders = shipments_df["Order_Id"].n_unique()
refund_orders  = refunds_df["Order_Id"].n_unique()
cancel_orders  = cancels_df["Order_Id"].n_unique()
total_units    = shipments_df["Quantity"].sum()
total_tax      = shipments_df["Total_Tax_Amount"].sum()

# ════════════════════════════════════════════════════════════════════════════════
# 1. REVENUE & SALES PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
header("1 · REVENUE & SALES PERFORMANCE")

subheader("Q1  Net revenue (gross shipments − refunds)")
print(f"    Gross revenue : {fmt_inr(gross_revenue)}")
print(f"    Refunds       : {fmt_inr(refund_amount)}")
print(f"    Net revenue   : {fmt_inr(net_revenue)}")

subheader("Q2  Gross-to-net gap")
gap_pct = safe_div(abs(refund_amount), gross_revenue) * 100
print(f"    {fmt_pct(gap_pct)} of gross revenue lost to refunds")

subheader("Q3–5  Core sales KPIs")
aov = safe_div(gross_revenue, shipped_orders)
asp = safe_div(gross_revenue, total_units)
print(f"    AOV (shipments only) : {fmt_inr(aov)}")
print(f"    Avg selling price    : {fmt_inr(asp)}")
print(f"    Total shipped units  : {total_units:,.0f}")

subheader("Q5–6  Daily & weekly revenue trend (top 7 days)")
daily_rev = (
    shipments_df
    .group_by("Date_Day")
    .agg(pl.col("Invoice_Amount").sum().alias("Revenue"))
    .sort("Date_Day")
)
print(daily_rev.head(7))

subheader("Q7  Revenue by fulfillment channel")
channel_rev = (
    shipments_df
    .group_by("Fulfillment_Channel")
    .agg(pl.col("Invoice_Amount").sum().alias("Revenue"))
    .sort("Revenue", descending=True)
)
print(channel_rev)

subheader("Q8  Revenue by seller GSTIN")
gstin_rev = (
    shipments_df
    .group_by("Seller_Gstin")
    .agg(pl.col("Invoice_Amount").sum().alias("Revenue"))
    .sort("Revenue", descending=True)
)
print(gstin_rev)

subheader("Q9  Revenue per fulfilled unit")
rev_per_unit = safe_div(net_revenue, total_units)
print(f"    {fmt_inr(rev_per_unit)} per unit")

subheader("Q10  Top-10 orders as % of total revenue (concentration)")
top10_rev = (
    shipments_df
    .group_by("Order_Id")
    .agg(pl.col("Invoice_Amount").sum().alias("Order_Revenue"))
    .sort("Order_Revenue", descending=True)
    .head(10)["Order_Revenue"]
    .sum()
)
print(f"    Top-10 orders = {fmt_pct(safe_div(top10_rev, gross_revenue) * 100)} of gross revenue")

# ════════════════════════════════════════════════════════════════════════════════
# 2. TRANSACTION QUALITY & HEALTH
# ════════════════════════════════════════════════════════════════════════════════
header("2 · TRANSACTION QUALITY & HEALTH")

subheader("Q11  Net shipment rate")
net_rate = safe_div(shipped_orders - refund_orders, shipped_orders) * 100
print(f"    {fmt_pct(net_rate)}  (shipped orders that were NOT refunded)")

subheader("Q12–13  Refund & cancellation rates")
print(f"    Refund rate      : {fmt_pct(safe_div(refund_orders, total_orders) * 100)}  ({refund_orders} orders)")
print(f"    Cancel rate      : {fmt_pct(safe_div(cancel_orders, total_orders) * 100)}  ({cancel_orders} orders)")

subheader("Q14  Cancel → re-ship cycles (same Order ID)")
cancel_ids  = set(cancels_df["Order_Id"].to_list())
ship_ids    = set(shipments_df["Order_Id"].to_list())
reship_ids  = cancel_ids & ship_ids
print(f"    {len(reship_ids)} orders had a cancel followed by a re-shipment")

subheader("Q15  Same-day refunds")
same_day_refunds = (
    refunds_df
    .filter(pl.col("Invoice_Date").dt.truncate("1d") == pl.col("Order_Date").dt.truncate("1d"))
    .select(pl.count("Order_Id").alias("Same_Day_Refunds"))
    .item()
)
print(f"    {same_day_refunds} refunds issued on the same day as order date")

subheader("Q16  High-value refund concentration")
high_val_threshold = gross_revenue / shipped_orders  # use AOV as threshold
high_val_refunds = refunds_df.filter(pl.col("Invoice_Amount").abs() > high_val_threshold)
print(f"    {high_val_refunds.height} refunds exceed AOV ({fmt_inr(high_val_threshold)})")

subheader("Q17  ⚠  Duplicate invoice numbers")
inv_counts = (
    df.filter(pl.col("Invoice_Number").is_not_null() & (pl.col("Invoice_Number") != ""))
    .group_by("Invoice_Number")
    .agg(pl.count("id").alias("Count"))
    .filter(pl.col("Count") > 1)
)
print(f"    {inv_counts.height} duplicate invoice numbers found")
if inv_counts.height:
    print(inv_counts.sort("Count", descending=True).head(5))

subheader("Q18  Orders with both Shipment and Refund (completed return loop)")
refund_set  = set(refunds_df["Order_Id"].to_list())
returned    = shipments_df.filter(pl.col("Order_Id").is_in(refund_set))
print(f"    {returned['Order_Id'].n_unique()} orders completed a full return loop")

subheader("Q19  Cancel-to-ship ratio by GSTIN")
gstin_cancel = (
    df.group_by("Seller_Gstin")
    .agg([
        (pl.col("Transaction_Type").is_in(CANCEL_TYPES).sum()).alias("Cancels"),
        (pl.col("Transaction_Type").is_in(SHIPMENT_TYPES).sum()).alias("Ships"),
    ])
    .with_columns(
        (pl.col("Cancels") / (pl.col("Ships") + pl.col("Cancels")) * 100).alias("Cancel_Rate_%")
    )
    .sort("Cancel_Rate_%", descending=True)
)
print(gstin_cancel)

subheader("Q20  Orders with multiple refund entries")
multi_refund = (
    refunds_df
    .group_by("Order_Id")
    .agg(pl.count("id").alias("Refund_Count"))
    .filter(pl.col("Refund_Count") > 1)
)
print(f"    {multi_refund.height} orders have more than one refund entry")

# ════════════════════════════════════════════════════════════════════════════════
# 3. PRODUCT & SKU INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════════════
header("3 · PRODUCT & SKU INTELLIGENCE")

sku_ship = (
    shipments_df
    .group_by("Sku")
    .agg([
        pl.col("Invoice_Amount").sum().alias("Gross_Revenue"),
        pl.col("Quantity").sum().alias("Units"),
        pl.col("Total_Tax_Amount").sum().alias("Tax"),
        pl.col("Order_Id").n_unique().alias("Orders"),
    ])
)
sku_refund = (
    refunds_df
    .group_by("Sku")
    .agg(pl.col("Invoice_Amount").sum().alias("Refund_Amount"))
)
sku_stats = (
    sku_ship
    .join(sku_refund, on="Sku", how="left")
    .with_columns(pl.col("Refund_Amount").fill_null(0))
    .with_columns(
        (pl.col("Gross_Revenue") + pl.col("Refund_Amount")).alias("Net_Revenue"),
        (pl.col("Refund_Amount").abs() / pl.col("Gross_Revenue") * 100).alias("Refund_Rate_%"),
    )
    .sort("Net_Revenue", descending=True)
)

subheader("Q21  Top 10 SKUs by net revenue")
print(sku_stats.select(["Sku", "Gross_Revenue", "Refund_Amount", "Net_Revenue"]).head(10))

subheader("Q22  SKUs with highest gross revenue but also highest refunds")
print(
    sku_stats
    .sort("Gross_Revenue", descending=True)
    .select(["Sku", "Gross_Revenue", "Refund_Amount", "Refund_Rate_%"])
    .head(10)
)

subheader("Q23  Top 10 SKUs by units sold")
print(sku_stats.sort("Units", descending=True).select(["Sku", "Units", "Net_Revenue"]).head(10))

subheader("Q24  ASINs across multiple seller GSTINs")
asin_gstin = (
    df.group_by("Asin")
    .agg(pl.col("Seller_Gstin").n_unique().alias("GSTIN_Count"))
    .filter(pl.col("GSTIN_Count") > 1)
    .sort("GSTIN_Count", descending=True)
)
print(f"    {asin_gstin.height} ASINs sold across more than one GSTIN")
print(asin_gstin.head(5))

subheader("Q25  Top HSN codes by taxable revenue")
print(
    shipments_df
    .group_by("Hsn/sac")
    .agg(pl.col("Tax_Exclusive_Gross").sum().alias("Taxable_Revenue"))
    .sort("Taxable_Revenue", descending=True)
    .head(5)
)

subheader("Q27  SKUs with zero refunds (most reliable)")
zero_refund_skus = sku_stats.filter(pl.col("Refund_Amount") == 0)
print(f"    {zero_refund_skus.height} SKUs have zero refunds")
print(zero_refund_skus.sort("Net_Revenue", descending=True).select(["Sku", "Net_Revenue", "Units"]).head(5))

subheader("Q28  ⚠  SKUs with high refund rates")
print(
    sku_stats
    .filter((pl.col("Refund_Rate_%") > 30) & (pl.col("Units") > 0))
    .sort("Refund_Rate_%", descending=True)
    .select(["Sku", "Refund_Rate_%", "Gross_Revenue", "Units"])
    .head(10)
)

subheader("Q30  Pareto (SKUs contributing 80% of net revenue)")
total_net = sku_stats["Net_Revenue"].sum()
pareto = sku_stats.with_columns(
    (pl.col("Net_Revenue").cum_sum() / total_net * 100).alias("Cumulative_%")
)
pareto_80 = pareto.filter(pl.col("Cumulative_%") <= 80)
print(f"    {pareto_80.height} SKUs drive 80% of net revenue")

subheader("Q31  High tax burden relative to revenue")
sku_tax_ratio = (
    sku_stats
    .with_columns((pl.col("Tax") / pl.col("Gross_Revenue") * 100).alias("Tax_Rate_%"))
    .sort("Tax_Rate_%", descending=True)
    .select(["Sku", "Tax_Rate_%", "Gross_Revenue", "Units"])
    .head(10)
)
print(sku_tax_ratio)

# ════════════════════════════════════════════════════════════════════════════════
# 4. CUSTOMER GEOGRAPHY
# ════════════════════════════════════════════════════════════════════════════════
header("4 · CUSTOMER GEOGRAPHY")

state_stats = (
    shipments_df
    .group_by("Ship_To_State")
    .agg([
        pl.col("Order_Id").n_unique().alias("Orders"),
        pl.col("Invoice_Amount").sum().alias("Revenue"),
        pl.col("Quantity").sum().alias("Units"),
    ])
    .sort("Orders", descending=True)
)
state_refunds = (
    refunds_df
    .group_by("Ship_To_State")
    .agg(pl.col("Order_Id").n_unique().alias("Refund_Orders"))
)
state_full = (
    state_stats
    .join(state_refunds, on="Ship_To_State", how="left")
    .with_columns(pl.col("Refund_Orders").fill_null(0))
    .with_columns(
        (pl.col("Refund_Orders") / pl.col("Orders") * 100).alias("Refund_Rate_%")
    )
)

subheader("Q33  Top 10 states by net revenue (after refunds)")
state_net = (
    state_full
    .join(
        refunds_df.group_by("Ship_To_State").agg(pl.col("Invoice_Amount").sum().alias("Refund_Amt")),
        on="Ship_To_State", how="left"
    )
    .with_columns(pl.col("Refund_Amt").fill_null(0))
    .with_columns((pl.col("Revenue") + pl.col("Refund_Amt")).alias("Net_Revenue"))
    .sort("Net_Revenue", descending=True)
)
print(state_net.select(["Ship_To_State", "Revenue", "Refund_Amt", "Net_Revenue"]).head(10))

subheader("Q34  Top 10 cities by order volume")
print(
    shipments_df
    .group_by("Ship_To_City")
    .agg(pl.col("Order_Id").n_unique().alias("Orders"))
    .sort("Orders", descending=True)
    .head(10)
)

subheader("Q36  ⚠  States with above-average refund rates")
avg_refund_rate = safe_div(refund_orders, shipped_orders) * 100
high_refund_states = state_full.filter(pl.col("Refund_Rate_%") > avg_refund_rate)
print(f"    National avg refund rate: {fmt_pct(avg_refund_rate)}")
print(high_refund_states.select(["Ship_To_State", "Orders", "Refund_Orders", "Refund_Rate_%"]))

subheader("Q37  Top 10 postal codes by revenue")
print(
    shipments_df
    .group_by("Ship_To_Postal_Code")
    .agg(pl.col("Invoice_Amount").sum().alias("Revenue"))
    .sort("Revenue", descending=True)
    .head(10)
)

subheader("Q39  Interstate vs intrastate shipment ratio")
intra = shipments_df.filter(pl.col("Is_Intrastate")).height
inter = shipments_df.height - intra
print(f"    Intrastate: {intra} ({fmt_pct(safe_div(intra, shipments_df.height) * 100)})")
print(f"    Interstate: {inter} ({fmt_pct(safe_div(inter, shipments_df.height) * 100)})")

# ════════════════════════════════════════════════════════════════════════════════
# 5. SHIPPING & ROUTE ANALYSIS
# ════════════════════════════════════════════════════════════════════════════════
header("5 · SHIPPING & ROUTE ANALYSIS")

subheader("Q42–43  Origin cities and states by shipment volume")
print(
    shipments_df
    .group_by("Ship_From_State")
    .agg(pl.count("Order_Id").alias("Shipments"))
    .sort("Shipments", descending=True)
    .head(5)
)

subheader("Q44  Top 10 state-to-state routes by volume")
routes = (
    shipments_df
    .group_by(["Ship_From_State", "Ship_To_State"])
    .agg(pl.count("Order_Id").alias("Volume"))
    .sort("Volume", descending=True)
    .head(10)
)
print(routes)

subheader("Q45  ⚠  Routes with high refund rates")
route_ship   = shipments_df.group_by(["Ship_From_State", "Ship_To_State"]).agg(pl.count("Order_Id").alias("Ship_Count"))
route_refund = refunds_df.group_by(["Ship_From_State", "Ship_To_State"]).agg(pl.count("Order_Id").alias("Refund_Count"))
route_quality = (
    route_ship
    .join(route_refund, on=["Ship_From_State", "Ship_To_State"], how="left")
    .with_columns(pl.col("Refund_Count").fill_null(0))
    .with_columns((pl.col("Refund_Count") / pl.col("Ship_Count") * 100).alias("Refund_Rate_%"))
    .filter(pl.col("Refund_Rate_%") > 20)
    .sort("Refund_Rate_%", descending=True)
)
print(route_quality.head(10))

# ════════════════════════════════════════════════════════════════════════════════
# 6. WAREHOUSE PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
header("6 · WAREHOUSE PERFORMANCE")

wh_ship = (
    shipments_df
    .group_by("Warehouse_Id")
    .agg([
        pl.col("Order_Id").n_unique().alias("Orders"),
        pl.col("Invoice_Amount").sum().alias("Revenue"),
    ])
)
wh_refund = (
    refunds_df
    .group_by("Warehouse_Id")
    .agg(pl.col("Order_Id").n_unique().alias("Refund_Orders"))
)
wh_stats = (
    wh_ship
    .join(wh_refund, on="Warehouse_Id", how="left")
    .with_columns(pl.col("Refund_Orders").fill_null(0))
    .with_columns(
        (pl.col("Refund_Orders") / pl.col("Orders") * 100).alias("Refund_Rate_%"),
        (pl.col("Revenue") / pl.col("Orders")).alias("Rev_per_Order"),
    )
    .sort("Revenue", descending=True)
)

subheader("Q49–55  Warehouse performance summary")
print(wh_stats)

# ════════════════════════════════════════════════════════════════════════════════
# 7. FULFILLMENT CHANNEL ANALYSIS
# ════════════════════════════════════════════════════════════════════════════════
header("7 · FULFILLMENT CHANNEL ANALYSIS")

fc_ship = (
    shipments_df
    .group_by("Fulfillment_Channel")
    .agg([
        pl.col("Order_Id").n_unique().alias("Orders"),
        pl.col("Invoice_Amount").sum().alias("Gross_Revenue"),
        pl.col("Quantity").sum().alias("Units"),
    ])
)
fc_refund = (
    refunds_df
    .group_by("Fulfillment_Channel")
    .agg([
        pl.col("Order_Id").n_unique().alias("Refund_Orders"),
        pl.col("Invoice_Amount").sum().alias("Refund_Amt"),
    ])
)
fc_stats = (
    fc_ship
    .join(fc_refund, on="Fulfillment_Channel", how="left")
    .with_columns([
        pl.col("Refund_Orders").fill_null(0),
        pl.col("Refund_Amt").fill_null(0),
    ])
    .with_columns([
        (pl.col("Gross_Revenue") + pl.col("Refund_Amt")).alias("Net_Revenue"),
        (pl.col("Refund_Orders") / pl.col("Orders") * 100).alias("Refund_Rate_%"),
        (pl.col("Gross_Revenue") / pl.col("Orders")).alias("AOV"),
    ])
    .sort("Net_Revenue", descending=True)
)

subheader("Q56–61  Fulfillment channel comparison")
print(fc_stats)

# ════════════════════════════════════════════════════════════════════════════════
# 8. TAX & COMPLIANCE ANALYSIS
# ════════════════════════════════════════════════════════════════════════════════
header("8 · TAX & COMPLIANCE ANALYSIS")

gross_excl = shipments_df["Tax_Exclusive_Gross"].sum()
effective_tax_rate = safe_div(total_tax, gross_excl) * 100

subheader("Q62–63  Tax totals and effective rate")
print(f"    Total tax collected   : {fmt_inr(total_tax)}")
print(f"    Tax-exclusive gross   : {fmt_inr(gross_excl)}")
print(f"    Effective tax rate    : {fmt_pct(effective_tax_rate)}")

subheader("Q64  Top 5 states by GST collected")
print(
    shipments_df
    .group_by("Ship_To_State")
    .agg(pl.col("Total_Tax_Amount").sum().alias("GST_Collected"))
    .sort("GST_Collected", descending=True)
    .head(5)
)

subheader("Q65  Top HSN codes by tax collected")
print(
    shipments_df
    .group_by("Hsn/sac")
    .agg(pl.col("Total_Tax_Amount").sum().alias("Tax"))
    .sort("Tax", descending=True)
    .head(5)
)

subheader("Q66  ⚠  Shipments with missing or zero HSN code")
missing_hsn = shipments_df.filter(
    pl.col("Hsn/sac").is_null() | (pl.col("Hsn/sac").cast(pl.Utf8).str.strip_chars() == "")
)
print(f"    {missing_hsn.height} shipment rows have a blank HSN/SAC code — compliance risk!")

subheader("Q67  Tax rate consistency per HSN code")
hsn_tax_check = (
    shipments_df
    .with_columns(
        (pl.col("Total_Tax_Amount") / pl.col("Tax_Exclusive_Gross") * 100).alias("Implied_Rate_%")
    )
    .group_by("Hsn/sac")
    .agg([
        pl.col("Implied_Rate_%").mean().alias("Avg_Rate_%"),
        pl.col("Implied_Rate_%").std().alias("Std_Dev"),
    ])
    .filter(pl.col("Std_Dev") > 1)          # flag HSNs with inconsistent rates
    .sort("Std_Dev", descending=True)
)
print(f"    {hsn_tax_check.height} HSN codes have inconsistent tax rates (std_dev > 1%)")
print(hsn_tax_check.head(5))

subheader("Q68  IGST vs CGST/SGST split (interstate vs intrastate)")
inter_tax = shipments_df.filter(~pl.col("Is_Intrastate"))["Total_Tax_Amount"].sum()
intra_tax = shipments_df.filter(pl.col("Is_Intrastate"))["Total_Tax_Amount"].sum()
print(f"    IGST (interstate)         : {fmt_inr(inter_tax)}")
print(f"    CGST+SGST (intrastate)    : {fmt_inr(intra_tax)}")

subheader("Q69  Refunds correctly reversing tax (negative tax check)")
positive_tax_refunds = refunds_df.filter(pl.col("Total_Tax_Amount") > 0)
print(f"    {positive_tax_refunds.height} refund rows have positive (non-reversed) tax — data issue!")

# ════════════════════════════════════════════════════════════════════════════════
# 9. ORDER QUALITY & DATA INTEGRITY
# ════════════════════════════════════════════════════════════════════════════════
header("9 · ORDER QUALITY & DATA INTEGRITY")

subheader("Q70  ⚠  Duplicate invoice numbers")
dup_inv = (
    df.filter(pl.col("Invoice_Number").is_not_null())
    .group_by("Invoice_Number")
    .agg(pl.count("id").alias("Count"))
    .filter(pl.col("Count") > 1)
)
print(f"    {dup_inv.height} duplicate invoice numbers across dataset")

subheader("Q71  ⚠  Missing invoice numbers")
missing_inv = df.filter(
    pl.col("Invoice_Number").is_null() | (pl.col("Invoice_Number") == "")
)
print(f"    {missing_inv.height} rows with missing invoice number")

subheader("Q72  Missing Warehouse IDs")
missing_wh = df.filter(pl.col("Warehouse_Id").is_null() | (pl.col("Warehouse_Id") == ""))
print(f"    {missing_wh.height} rows with missing Warehouse ID")

subheader("Q73–74  Quantity statistics")
avg_qty = safe_div(total_units, shipped_orders)
max_qty = shipments_df["Quantity"].max()
print(f"    Avg quantity per order : {avg_qty:.2f}")
print(f"    Max quantity in one row: {max_qty}")

subheader("Q75  ⚠  Shipments with zero invoice amount")
zero_amt_ships = shipments_df.filter(pl.col("Invoice_Amount") == 0)
print(f"    {zero_amt_ships.height} shipment rows have zero invoice amount")

subheader("Q76  ⚠  Negative invoice amounts on non-refund rows")
bad_negatives = (
    df.filter(~pl.col("Transaction_Type").is_in(REFUND_TYPES))
    .filter(pl.col("Invoice_Amount") < 0)
)
print(f"    {bad_negatives.height} non-refund rows have negative invoice amount")

subheader("Q77  Refunds without a matching shipment")
orphan_refunds = refunds_df.filter(~pl.col("Order_Id").is_in(ship_ids))
print(f"    {orphan_refunds.height} refund rows have no matching shipment in dataset")

# ════════════════════════════════════════════════════════════════════════════════
# 10. OPERATIONAL & RESTOCK INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════════════
header("10 · OPERATIONAL & RESTOCK INTELLIGENCE")

subheader("Q78  🔑 SKUs to restock first (high velocity + low refund rate)")
restock_candidates = (
    sku_stats
    .filter((pl.col("Units") > sku_stats["Units"].mean()) & (pl.col("Refund_Rate_%") < 10))
    .sort("Units", descending=True)
    .select(["Sku", "Units", "Net_Revenue", "Refund_Rate_%"])
    .head(10)
)
print(restock_candidates)

subheader("Q79  Products with low sales but zero refunds (possible stockout signal)")
possible_stockout = (
    sku_stats
    .filter((pl.col("Units") < sku_stats["Units"].quantile(0.25)) & (pl.col("Refund_Amount") == 0))
    .sort("Units")
    .select(["Sku", "Units", "Net_Revenue"])
    .head(10)
)
print(possible_stockout)

subheader("Q82  ⚠  Fulfillment channel causing most revenue leakage")
print(fc_stats.sort("Refund_Amt").select(["Fulfillment_Channel", "Refund_Amt", "Refund_Rate_%"]).head(5))

subheader("Q84  ⚠  GSTINs needing compliance review")
print(gstin_cancel.filter(pl.col("Cancel_Rate_%") > 10))

# ════════════════════════════════════════════════════════════════════════════════
# 11. EXECUTIVE SUMMARY
# ════════════════════════════════════════════════════════════════════════════════
header("11 · EXECUTIVE SUMMARY")

top_sku        = sku_stats["Sku"][0]
top_sku_rev    = sku_stats["Net_Revenue"][0]
top_state      = state_net["Ship_To_State"][0]
top_channel    = fc_stats["Fulfillment_Channel"][0]
total_leakage  = abs(refund_amount)
refund_rate    = safe_div(refund_orders, total_orders) * 100
net_ship_rate  = safe_div(shipped_orders - refund_orders, shipped_orders) * 100

print(f"""
  ┌─────────────────────────────────────────────────────┐
  │  BUSINESS HEALTH SNAPSHOT                           │
  ├─────────────────────────────────────────────────────┤
  │  Gross Revenue      : {fmt_inr(gross_revenue):<28} │
  │  Net Revenue        : {fmt_inr(net_revenue):<28} │
  │  Revenue Leakage    : {fmt_inr(total_leakage):<28} │
  │  Refund Rate        : {fmt_pct(refund_rate):<28} │
  │  Net Shipment Rate  : {fmt_pct(net_ship_rate):<28} │
  │  Total Tax          : {fmt_inr(total_tax):<28} │
  ├─────────────────────────────────────────────────────┤
  │  Top SKU            : {top_sku:<28} │
  │  Top SKU Net Rev    : {fmt_inr(top_sku_rev):<28} │
  │  Top State          : {top_state:<28} │
  │  Best Channel       : {top_channel:<28} │
  │  Pareto SKU Count   : {str(pareto_80.height) + ' SKUs → 80% revenue':<28} │
  └─────────────────────────────────────────────────────┘
""")

print("  Top 3 restock priorities:")
for i, row in enumerate(restock_candidates.head(3).iter_rows(named=True), 1):
    print(f"    {i}. {row['Sku']}  — {row['Units']:.0f} units sold, {fmt_pct(row['Refund_Rate_%'])} refund rate")

print("\n  Top 3 margin destroyers (high refund rate):")
worst = sku_stats.filter(pl.col("Refund_Rate_%") > 0).sort("Refund_Rate_%", descending=True).head(3)
for i, row in enumerate(worst.iter_rows(named=True), 1):
    print(f"    {i}. {row['Sku']}  — {fmt_pct(row['Refund_Rate_%'])} refund rate, {fmt_inr(row['Gross_Revenue'])} gross")

print(f"\n  AFN dependency: {fc_stats.filter(pl.col('Fulfillment_Channel') == 'AFN')['Orders'].sum()} AFN orders"
      f" vs {fc_stats.filter(pl.col('Fulfillment_Channel') == 'MFN')['Orders'].sum()} MFN orders")

print("\n" + "═" * 70)
print(f"  Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("═" * 70)