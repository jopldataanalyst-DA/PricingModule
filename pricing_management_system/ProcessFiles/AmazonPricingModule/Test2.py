"""Amazon rate-card workbook prototype.

Use case:
    Loads AmazonRateCard.xlsx sheets and tests the category/range/fee lookup
    functions that were later consolidated into AmazonPricing.py.
"""

import polars as pl

RATE_CARD_PATH = r"D:\VatsalFiles\PricingModule\Data\AmazonData\AmazonRateCard.xlsx"


def load_sheet(sheet_name):
    return pl.read_excel(
        RATE_CARD_PATH,
        sheet_name=sheet_name
    )


def load_rate_card():
    category_mapping = load_sheet("CategoryMapping").with_columns([
        pl.col("Category").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
        pl.col("Amazon Cat").cast(pl.Utf8).str.strip_chars(),
    ])

    commission = load_sheet("Commission")
    whf = load_sheet("WHF")
    refund = load_sheet("Refund")
    fix_closing_fee = load_sheet("Fix Closing Fee")
    extra_fee = load_sheet("Extra Fee")
    shipping = load_sheet("Shipping")

    return {
        "category_mapping": category_mapping,
        "commission": commission,
        "whf": whf,
        "refund": refund,
        "fix_closing_fee": fix_closing_fee,
        "extra_fee": extra_fee,
        "shipping": shipping,
    }


def get_price_range(price):
    if price <= 300:
        return "0-300"
    elif price <= 500:
        return "300-500"
    elif price <= 1000:
        return "500-1000"
    else:
        return ">1000"


def get_fixed_fee_range(price):
    if price <= 250:
        return "0-250"
    elif price <= 500:
        return "250-500"
    elif price <= 1000:
        return "500-1000"
    else:
        return ">1000"


def get_category(data, category):
    category_key = category.strip().lower()

    row = data["category_mapping"].filter(
        pl.col("Category") == category_key
    )

    if row.height == 0:
        return category.strip()

    return row["Amazon Cat"][0]


def get_matrix_value(df, row_name, column_name):
    row = df.filter(
        pl.col(df.columns[0]).cast(pl.Utf8).str.strip_chars() == row_name
    )

    if row.height == 0:
        raise ValueError(f"Row not found: {row_name}")

    return float(row[column_name][0])


def get_charge_value(df, column_name):
    return float(df[column_name][0])


def get_extra_fee(data, fee_name, range_name):
    row = data["extra_fee"].filter(
        pl.col(data["extra_fee"].columns[0]).cast(pl.Utf8).str.strip_chars() == fee_name
    )

    if row.height == 0:
        raise ValueError(f"Extra fee not found: {fee_name}")

    return float(row[range_name][0])


def calculate_price(x_value, category, include_refund=False):
    data = load_rate_card()

    input_category = category.strip()
    amazon_category = get_category(data, input_category)

    selling_price = x_value

    for _ in range(50):
        price_range = get_price_range(selling_price)
        fixed_range = get_fixed_fee_range(selling_price)

        commission_rate = get_matrix_value(
            data["commission"],
            amazon_category,
            price_range
        )

        fixed_closing_fee = get_charge_value(
            data["fix_closing_fee"],
            fixed_range
        )

        fba_pick_pack = get_extra_fee(
            data,
            "FBA Pick And Pack",
            fixed_range
        )

        technology_fee = get_extra_fee(
            data,
            "Technology Fee",
            fixed_range
        )

        shipping_fee = get_matrix_value(
            data["shipping"],
            amazon_category,
            price_range
        )

        whf_percent = get_matrix_value(
            data["whf"],
            amazon_category,
            price_range
        )

        shipping_fee_charged = shipping_fee * whf_percent

        refund_charge = (
            get_charge_value(data["refund"], price_range)
            if include_refund
            else 0
        )

        fixed_charges = (
            fixed_closing_fee
            + fba_pick_pack
            + technology_fee
            + shipping_fee_charged
            + refund_charge
        )

        new_selling_price = (
            x_value + fixed_charges
        ) / (1 - commission_rate)

        if round(new_selling_price) == round(selling_price):
            selling_price = new_selling_price
            break

        selling_price = new_selling_price

    price_range = get_price_range(selling_price)
    fixed_range = get_fixed_fee_range(selling_price)

    commission_rate = get_matrix_value(data["commission"], amazon_category, price_range)
    fixed_closing_fee = get_charge_value(data["fix_closing_fee"], fixed_range)
    fba_pick_pack = get_extra_fee(data, "FBA Pick And Pack", fixed_range)
    technology_fee = get_extra_fee(data, "Technology Fee", fixed_range)

    shipping_fee = get_matrix_value(data["shipping"], amazon_category, price_range)
    whf_percent = get_matrix_value(data["whf"], amazon_category, price_range)
    shipping_fee_charged = shipping_fee * whf_percent

    refund_charge = (
        get_charge_value(data["refund"], price_range)
        if include_refund
        else 0
    )

    commission_amount = selling_price * commission_rate

    total_charges = (
        commission_amount
        + fixed_closing_fee
        + fba_pick_pack
        + technology_fee
        + shipping_fee_charged
        + refund_charge
    )

    final_value = selling_price - total_charges

    return {
        "Input X Value": round(x_value, 2),
        "Input Category": input_category,
        "Amazon Category": amazon_category,
        "Selected Price Range": price_range,
        "Selected Fixed Fee Range": fixed_range,
        "Required Selling Price": round(selling_price),
        "Commission %": f"{commission_rate * 100}%",
        "Commission Amount": round(commission_amount, 2),
        "Fixed Closing Fee": fixed_closing_fee,
        "FBA Pick Pack": fba_pick_pack,
        "Technology Fee": technology_fee,
        "Full Shipping Fee": shipping_fee,
        "WHF % On Shipping": f"{whf_percent * 100}%",
        "Shipping Fee Charged": shipping_fee_charged,
        "Refund Charge": refund_charge,
        "Total Charges": round(total_charges, 2),
        "Final Value After Charges": round(final_value, 2),
    }


result = calculate_price(
    x_value=450,
    category="SAREE",
    include_refund=False
)

for key, value in result.items():
    print(f"{key}: {value}")
