"""Early standalone Amazon pricing calculator experiment.

Use case:
    Contains hard-coded category/rate-card data and helper functions used while
    prototyping the Amazon pricing formula. Kept for reference; production
    calculations are in AmazonPricing.py.
"""

# Selling Price - Charges = X

CATEGORY_MAPPING = {
    "KURTA-ST": "Apparel - Ethnic Wear",
    "KURTA-JPR": "Apparel - Ethnic Wear",
    "SKD-ST": "Apparel - Ethnic Wear",
    "SKD-JPR": "Apparel - Ethnic Wear",
    "SAREE": "Apparel - Sarees and Dress Materials",
    "dress material": "Apparel - Sarees and Dress Materials",
    "Dress 46\"": "Apparel - Dress",
    "DRESS 42 \"": "Apparel - Dress",
    "DRESS 36\"": "Apparel - Dress",
    "TUNICS-ST": "Apparel - Other products",
    "TUNICS-JPR": "Apparel - Other products",
    "WESTERN TOP": "Apparel - Other products",
    "COTTON TOP": "Apparel - Other products",
    "Crop top": "Apparel - Other products",
    "Cord Set": "Apparel - Ethnic Wear",
}


# ============================================================
# PRICE RANGE
# ============================================================

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


# ============================================================
# COMMISSION
# ============================================================

COMMISSION = {
    "Apparel - Ethnic Wear": {
        "0-300": 0.00,
        "300-500": 0.01,
        "500-1000": 0.10,
        ">1000": 0.16,
    },

    "Apparel - Sarees and Dress Materials": {
        "0-300": 0.00,
        "300-500": 0.04,
        "500-1000": 0.08,
        ">1000": 0.23,
    },

    "Apparel - Dress": {
        "0-300": 0.00,
        "300-500": 0.05,
        "500-1000": 0.07,
        ">1000": 0.19,
    },

    "Apparel - Other products": {
        "0-300": 0.00,
        "300-500": 0.05,
        "500-1000": 0.11,
        ">1000": 0.19,
    },
}


# ============================================================
# OTHER CHARGES
# ============================================================

REFUND_CHARGE = {
    "0-300": 45,
    "300-500": 70,
    "500-1000": 95,
    ">1000": 130,
}

FIX_CLOSING_FEE = {
    "0-250": 25,
    "250-500": 20,
    "500-1000": 20,
    ">1000": 25,
}

FBA_PICK_PACK = {
    "0-250": 25,
    "250-500": 20,
    "500-1000": 20,
    ">1000": 25,
}

TECHNOLOGY_FEE = {
    "0-250": 12,
    "250-500": 13,
    "500-1000": 14,
    ">1000": 15,
}

SHIPPING_FEE = {
    "0-250": 20,
    "250-500": 30,
    "500-1000": 40,
    ">1000": 50,
}


# ============================================================
# MAIN FUNCTION
# ============================================================

def calculate_price(x_value, category, include_refund=False):

    category = category.strip()

    amazon_category = CATEGORY_MAPPING.get(category, category)

    if amazon_category not in COMMISSION:
        raise ValueError(f"Category not found: {amazon_category}")

    # Initial selling price
    selling_price = x_value

    # Loop until correct selling price found
    for _ in range(20):

        # Range selection
        price_range = get_price_range(selling_price)
        fixed_range = get_fixed_fee_range(selling_price)

        # Commission
        commission_rate = COMMISSION[amazon_category][price_range]

        # Charges
        fixed_closing_fee = FIX_CLOSING_FEE[fixed_range]
        fba_pick_pack = FBA_PICK_PACK[fixed_range]
        technology_fee = TECHNOLOGY_FEE[fixed_range]
        shipping_fee = SHIPPING_FEE[fixed_range] * 0.4

        # Refund
        refund_charge = (
            REFUND_CHARGE[price_range]
            if include_refund
            else 0
        )

        # Total fixed charges
        fixed_charges = (
            fixed_closing_fee
            + fba_pick_pack
            + technology_fee
            + shipping_fee
            + refund_charge
        )

        # Formula:
        # Selling Price - Charges = X
        # Charges include commission
        new_selling_price = (
            x_value + fixed_charges
        ) / (1 - commission_rate)

        # Stop if stable
        if round(new_selling_price) == round(selling_price):
            selling_price = new_selling_price
            break

        selling_price = new_selling_price

    # ========================================================
    # FINAL CALCULATION
    # ========================================================

    price_range = get_price_range(selling_price)
    fixed_range = get_fixed_fee_range(selling_price)

    commission_rate = COMMISSION[amazon_category][price_range]

    fixed_closing_fee = FIX_CLOSING_FEE[fixed_range]
    fba_pick_pack = FBA_PICK_PACK[fixed_range]
    technology_fee = TECHNOLOGY_FEE[fixed_range]
    shipping_fee = SHIPPING_FEE[fixed_range] * 0.4

    refund_charge = (
        REFUND_CHARGE[price_range]
        if include_refund
        else 0
    )

    # Commission Amount
    commission_amount = selling_price * commission_rate

    # Total Charges
    total_charges = (
        commission_amount
        + fixed_closing_fee
        + fba_pick_pack
        + technology_fee
        + shipping_fee
        + refund_charge
    )

    # Final Value
    final_value = selling_price - total_charges

    # ========================================================
    # OUTPUT
    # ========================================================

    return {
        "Input X Value": round(x_value, 2),
        "Input Category": category,
        "Amazon Category": amazon_category,

        "Selected Price Range": price_range,
        "Selected Fixed Fee Range": fixed_range,

        "Required Selling Price": round(selling_price),

        "Commission %": f"{commission_rate * 100}%",
        "Commission Amount": round(commission_amount, 2),

        "Fixed Closing Fee": fixed_closing_fee,
        "FBA Pick Pack": fba_pick_pack,
        "Technology Fee": technology_fee,
        "Shipping Fee": shipping_fee,

        "Refund Charge": refund_charge,

        "Total Charges": round(total_charges, 2),

        "Final Value After Charges": round(final_value, 2),
    }


# ============================================================
# TEST
# ============================================================

result = calculate_price(
    x_value=450,
    category="SAREE",
    include_refund=False
)

print()

for key, value in result.items():
    print(f"{key}: {value}")
