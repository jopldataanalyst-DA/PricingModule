"""Amazon pricing calculation engine.

Use case:
    Reads stock_items from MySQL and Amazon rate-card sheets, maps each product
    to an Amazon category, calculates fees/required selling price/profit fields,
    and returns the result DataFrame consumed by data_pipeline.run_amazon_pipeline.
"""

import duckdb
import polars as pl
import sys
from pathlib import Path

# ============================================================
# SETTINGS
# ============================================================
DbConfig = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module",
}

TableName = "stock_items"

RateCardPath = r"D:\VatsalFiles\PricingModule\Data\AmazonData\AmazonRateCard.xlsx"
OutputPath = r"D:\VatsalFiles\PricingModule\Data\AmazonData\Amazon_New_Pricing_Result.xlsx"
CsvOutputPath = r"D:\VatsalFiles\PricingModule\Data\AmazonData\Amazon_Pricing_Result.csv"

DefaultAmazonCategory = "Apparel - Other products"

CostIntoPercent = 23
ReturnCharge = 59
GstPercent = 18

_LOCAL_DATABASE = None


def GetDatabase():
    """Use the application pool when available, otherwise create a local helper."""
    global _LOCAL_DATABASE
    try:
        from database import get_database
        return get_database()
    except ImportError:
        if _LOCAL_DATABASE is not None:
            return _LOCAL_DATABASE
        database_module_dir = Path(__file__).parent.parent / "DatabaseModule"
        if str(database_module_dir) not in sys.path:
            sys.path.append(str(database_module_dir))
        from AdvanceDatabase import MySqlDatabase
        _LOCAL_DATABASE = MySqlDatabase(DbConfig, PoolName="AmazonPricingPool")
        return _LOCAL_DATABASE


# ============================================================
# HELPERS
# ============================================================
def Num(Value):
    """Convert spreadsheet values to float, treating blanks/errors as zero."""
    try:
        if Value is None:
            return 0.0
        Value = str(Value).replace(",", "").replace("%", "").strip()
        return float(Value) if Value else 0.0
    except Exception:
        return 0.0


def Percent(Value):
    """Normalize a percent value so 23 and 0.23 both become 0.23."""
    Value = Num(Value)
    return Value / 100 if Value > 1 else Value


def PriceRange(Price):
    """Map a selling price to Amazon commission range labels."""
    Price = Num(Price)
    if Price <= 300:
        return "0-300"
    if Price <= 500:
        return "300-500"
    if Price <= 1000:
        return "500-1000"
    return ">1000"


def FixedFeeRange(Price):
    """Map a selling price to Amazon fixed-closing-fee range labels."""
    Price = Num(Price)
    if Price <= 250:
        return "0-250"
    if Price <= 500:
        return "250-500"
    if Price <= 1000:
        return "500-1000"
    return ">1000"


# ============================================================
# LOAD MYSQL DATA USING DUCKDB
# ============================================================
def LoadStockData():
    """Load source product/stock/pricing fields from stock_items."""
    rows = GetDatabase().FetchAll(f"""
        SELECT
            sku_code AS Master_SKU,
            category AS Category,
            parent_remark AS Remark,
            cost AS Cost,
            mrp AS MRP,
            available_atp AS Uniware,
            fba_stock AS FBA,
            sjit_stock AS Sjit,
            fbf_stock AS FBF,
            location AS LOC,
            price AS Current_Price,
            updated AS Launch_Date
        FROM {TableName}
    """)

    if not rows:
        return pl.DataFrame(schema={
            "Master_SKU": pl.Utf8,
            "Category": pl.Utf8,
            "Remark": pl.Utf8,
            "Cost": pl.Float64,
            "MRP": pl.Float64,
            "Uniware": pl.Float64,
            "FBA": pl.Float64,
            "Sjit": pl.Float64,
            "FBF": pl.Float64,
            "LOC": pl.Utf8,
            "Current_Price": pl.Float64,
            "Launch_Date": pl.Utf8
        })
    return pl.from_dicts(rows)


# ============================================================
# LOAD RATE CARD USING POLARS
# ============================================================
def LoadSheet(Sheet):
    return pl.read_excel(RateCardPath, sheet_name=Sheet)


def LoadRateCard():
    CategoryMapping = (
        LoadSheet("CategoryMapping")
        .with_columns([
            pl.col("Category").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            pl.col("Amazon Cat").cast(pl.Utf8).str.strip_chars(),
        ])
    )

    return {
        "category_mapping": CategoryMapping,
        "commission": LoadSheet("Commission"),
        "whf": LoadSheet("WHF"),
        "refund": LoadSheet("Refund"),
        "fix_closing_fee": LoadSheet("Fix Closing Fee"),
        "extra_fee": LoadSheet("Extra Fee"),
        "shipping": LoadSheet("Shipping"),
    }


def GetCategory(Data, Category):
    Key = str(Category).strip().lower()

    Row = Data["category_mapping"].filter(pl.col("Category") == Key)

    if Row.height == 0:
        return DefaultAmazonCategory

    return Row["Amazon Cat"][0]


def MatrixValue(Df, RowName, ColName):
    if ColName not in Df.columns:
        return 0.0

    FirstCol = Df.columns[0]

    Row = Df.filter(
        pl.col(FirstCol).cast(pl.Utf8).str.strip_chars() == str(RowName).strip()
    )

    if Row.height == 0:
        return 0.0

    return Num(Row[ColName][0])


def FirstRowValue(Df, ColName):
    if ColName not in Df.columns or Df.height == 0:
        return 0.0

    return Num(Df[ColName][0])


def ExtraFee(Data, FeeName, RangeName):
    Df = Data["extra_fee"]
    FirstCol = Df.columns[0]

    Row = Df.filter(
        pl.col(FirstCol).cast(pl.Utf8).str.strip_chars() == FeeName
    )

    if Row.height == 0 or RangeName not in Df.columns:
        return 0.0

    return Num(Row[RangeName][0])


# ============================================================
# AMAZON PRICE CALCULATION
# Final TP is X value
# Required Selling Price - Charges = Final TP
# ============================================================
def CalculatePrice(Data, FinalTp, Category, IncludeRefund=False):
    FinalTp = Num(FinalTp)
    AmazonCat = GetCategory(Data, Category)

    SellingPrice = FinalTp

    for _ in range(100):
        Pr = PriceRange(SellingPrice)
        Fr = FixedFeeRange(SellingPrice)

        CommissionRate = Percent(MatrixValue(Data["commission"], AmazonCat, Pr))

        FixedClosingFee = FirstRowValue(Data["fix_closing_fee"], Fr)
        FbaPickPack = ExtraFee(Data, "FBA Pick And Pack", Fr)
        TechnologyFee = ExtraFee(Data, "Technology Fee", Fr)

        FullShippingFee = MatrixValue(Data["shipping"], AmazonCat, Pr)
        WhfPercent = Percent(MatrixValue(Data["whf"], AmazonCat, Pr))
        ShippingFeeCharged = FullShippingFee * WhfPercent

        RefundCharge = FirstRowValue(Data["refund"], Pr) if IncludeRefund else 0

        FixedCharges = (
            FixedClosingFee
            + FbaPickPack
            + TechnologyFee
            + ShippingFeeCharged
            + RefundCharge
        )

        if CommissionRate >= 1:
            CommissionRate = 0

        NewPrice = (FinalTp + FixedCharges) / (1 - CommissionRate)

        if round(NewPrice) == round(SellingPrice):
            SellingPrice = NewPrice
            break

        SellingPrice = NewPrice

    Pr = PriceRange(SellingPrice)
    Fr = FixedFeeRange(SellingPrice)

    CommissionRate = Percent(MatrixValue(Data["commission"], AmazonCat, Pr))
    CommissionAmount = SellingPrice * CommissionRate

    FixedClosingFee = FirstRowValue(Data["fix_closing_fee"], Fr)
    FbaPickPack = ExtraFee(Data, "FBA Pick And Pack", Fr)
    TechnologyFee = ExtraFee(Data, "Technology Fee", Fr)

    FullShippingFee = MatrixValue(Data["shipping"], AmazonCat, Pr)
    WhfPercent = Percent(MatrixValue(Data["whf"], AmazonCat, Pr))
    ShippingFeeCharged = FullShippingFee * WhfPercent

    RefundCharge = FirstRowValue(Data["refund"], Pr) if IncludeRefund else 0

    TotalCharges = (
        CommissionAmount
        + FixedClosingFee
        + FbaPickPack
        + TechnologyFee
        + ShippingFeeCharged
        + RefundCharge
    )

    FinalValue = SellingPrice - TotalCharges

    return {
        "Amazon Cat": AmazonCat,
        "Selected Price Range": Pr,
        "Selected Fixed Fee Range": Fr,
        "Required Selling Price": round(SellingPrice),
        "Commission %": round(CommissionRate * 100, 2),
        "Commission Amount": round(CommissionAmount, 2),
        "Fixed Closing Fee": FixedClosingFee,
        "FBA Pick Pack": FbaPickPack,
        "Technology Fee": TechnologyFee,
        "Full Shipping Fee": FullShippingFee,
        "WHF % On Shipping": round(WhfPercent * 100, 2),
        "Shipping Fee Charged": round(ShippingFeeCharged, 2),
        "Refund Charge": RefundCharge,
        "Total Charges": round(TotalCharges, 2),
        "Final Value After Charges": round(FinalValue, 2),
    }


# ============================================================
# MAIN PROCESS
# ============================================================
def run_amazon_pricing():
    Stock = LoadStockData()
    RateData = LoadRateCard()

    Stock = Stock.with_columns([
        pl.col("Master_SKU").cast(pl.Utf8).str.strip_chars(),
        pl.col("Category").cast(pl.Utf8).str.strip_chars(),
        pl.col("Remark").cast(pl.Utf8).str.strip_chars(),
        pl.col("LOC").cast(pl.Utf8).str.strip_chars(),

        pl.col("Cost").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("MRP").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("Uniware").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("FBA").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("Sjit").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("FBF").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0),
    ])

    Stock = Stock.with_columns([
        pl.when(pl.col("Cost") > 0)
        .then(pl.col("Cost") / (100 - CostIntoPercent) * 100)
        .otherwise(0)
        .alias("Cost after %"),

        pl.when(pl.col("Cost") > 0)
        .then(ReturnCharge * GstPercent / 100)
        .otherwise(0)
        .alias("GST on Return"),
    ])

    Stock = Stock.with_columns([
        pl.when(pl.col("Cost") > 0)
        .then(pl.col("Cost after %") + ReturnCharge + pl.col("GST on Return"))
        .otherwise(0)
        .alias("Final TP")
    ])

    Stock = Stock.with_columns(pl.lit(float(ReturnCharge)).alias('Return Charge'))
    Stock = Stock.with_columns(pl.lit(float(CostIntoPercent)).alias('Cost into %'))

    PricingRows = [
        CalculatePrice(
            Data=RateData,
            FinalTp=Row["Final TP"],
            Category=Row["Category"],
            IncludeRefund=False,
        )
        for Row in Stock.iter_rows(named=True)
    ]

    Pricing = pl.DataFrame(PricingRows)

    FinalDf = pl.concat(
        [Stock, Pricing],
        how="horizontal"
    )

    FinalDf = FinalDf.rename({
        "Category": "Original_Category",
        "Launch_Date": "Launch Date",
    })

    FinalDf = FinalDf.with_columns((pl.col('Required Selling Price') - pl.col('Total Charges')).alias('Sett Acc Panel'))

    FinalDf = FinalDf.with_columns((pl.col('Sett Acc Panel') - pl.col('Return Charge') - pl.col('GST on Return') - pl.col('Cost')).alias('Net Profit On SP'))

    FinalDf = FinalDf.with_columns((pl.col('Net Profit On SP')/pl.col('Required Selling Price')*100).alias('Net Profit % On SP'))

    FinalDf = FinalDf.with_columns(
        ((pl.col('Net Profit On SP') / pl.col('Final TP')) * 100).alias('Net Profit % On TP')
    )
    
    Olddf = pl.read_excel(rf"D:\VatsalFiles\PricingModule\Data\OldPricing.xlsx")
    FinalDf = FinalDf.join(Olddf, left_on="Master_SKU", right_on="master_sku", how="left")
    
    ColumnsOrder = [
        "Master_SKU",
        "Original_Category",
        "Amazon Cat",
        "Remark",
        "Cost",
        "MRP",
        "Uniware",
        "FBA",
        "Sjit",
        "FBF",
        "Launch Date",
        "LOC",
        "Cost into %",
        "Cost after %",
        "Return Charge",
        "GST on Return",
        "Final TP",
        "Required Selling Price",
        "Selected Price Range",
        "Selected Fixed Fee Range",
        "Commission %",
        "Commission Amount",
        "Fixed Closing Fee",
        "FBA Pick Pack",
        "Technology Fee",
        "Full Shipping Fee",
        "WHF % On Shipping",
        "Shipping Fee Charged",
        "Total Charges",
        "Final Value After Charges",
        "Sett Acc Panel",
        "Net Profit On SP",
        "Net Profit % On SP",
        "Net Profit % On TP",
        "Old Daily SP",
        "Old Deal SP",
    ]

    return FinalDf.select([
        Col for Col in ColumnsOrder if Col in FinalDf.columns
    ])


if __name__ == "__main__":
    FinalDf = run_amazon_pricing()

    FinalDf.write_csv(CsvOutputPath)
    FinalDf.write_excel(OutputPath)

    print("Process Completed Successfully!")
    print(f"Excel Output: {OutputPath}")
    print(f"CSV Output: {CsvOutputPath}")
    print(f"Total Rows: {FinalDf.height}")

    print(
        FinalDf.select([
            "Master_SKU",
            "Original_Category",
            "Amazon Cat",
            "Final TP",
            "Required Selling Price",
            "Final Value After Charges",
        ]).head(10)
    )
