from fastapi import APIRouter, Depends, Query
from typing import List, Dict, Any
from auth import get_current_user
from database import get_db
import json
import math

router = APIRouter()

ALL_COLUMNS = [
    "master_sku", "item_name", "original_category", "amazon_cat", "remark",
    "cost", "mrp", "uniware", "fba", "sjit", "fbf",
    "launch_date", "loc", "cost_into_percent", "cost_after_percent",
    "return_charge", "gst_on_return", "final_tp", "required_selling_price",
    "selected_price_range", "selected_fixed_fee_range", "commission_percent",
    "commission_amount", "fixed_closing_fee", "fba_pick_pack", "technology_fee",
    "full_shipping_fee", "whf_percent_on_shipping", "shipping_fee_charged",
    "total_charges", "final_value_after_charges", "old_daily_sp", "old_deal_sp", "sett_acc_panel",
    "net_profit_on_sp", "net_profit_percent_on_sp", "net_profit_percent_on_tp"
]

DEFAULT_VISIBLE_COLUMNS = [
    "master_sku", "item_name", "original_category", "amazon_cat", "remark",
    "cost", "mrp", "uniware", "fba", "sjit", "fbf",
    "launch_date", "loc", "cost_into_percent", "cost_after_percent",
    "return_charge", "gst_on_return", "final_tp", "required_selling_price",
    "sett_acc_panel", "net_profit_on_sp", "net_profit_percent_on_sp",
    "net_profit_percent_on_tp", "old_daily_sp", "old_deal_sp"
]

COLUMN_LABELS = {
    "master_sku": "Master SKU",
    "item_name": "Style ID / Parent SKU",
    "original_category": "Original Category",
    "amazon_cat": "Amazon Cat",
    "remark": "Remark",
    "cost": "Cost",
    "mrp": "MRP",
    "uniware": "Uniware",
    "fba": "FBA",
    "sjit": "Sjit",
    "fbf": "FBF",
    "launch_date": "Launch Date",
    "loc": "LOC",
    "cost_into_percent": "Cost into %",
    "cost_after_percent": "Cost after %",
    "return_charge": "Return Charge",
    "gst_on_return": "GST on Return",
    "final_tp": "Final TP",
    "required_selling_price": "Required Selling Price",
    "selected_price_range": "Selected Price Range",
    "selected_fixed_fee_range": "Selected Fixed Fee Range",
    "commission_percent": "Commission %",
    "commission_amount": "Commission Amount",
    "fixed_closing_fee": "Fixed Closing Fee",
    "fba_pick_pack": "FBA Pick Pack",
    "technology_fee": "Technology Fee",
    "full_shipping_fee": "Full Shipping Fee",
    "whf_percent_on_shipping": "WHF % On Shipping",
    "shipping_fee_charged": "Shipping Fee Charged",
    "total_charges": "Total Charges",
    "final_value_after_charges": "Final Value After Charges",
    "old_daily_sp": "Old Daily SP",
    "old_deal_sp": "Old Deal SP",
    "sett_acc_panel": "Sett Acc Panel",
    "net_profit_on_sp": "Net Profit On SP",
    "net_profit_percent_on_sp": "Net Profit % On SP",
    "net_profit_percent_on_tp": "Net Profit % On TP"
}

NUMERIC_COLUMNS = [
    'id', 'cost', 'mrp', 'uniware', 'fba', 'sjit', 'fbf', 'cost_into_percent', 'cost_after_percent',
    'return_charge', 'gst_on_return', 'final_tp', 'required_selling_price', 'commission_percent',
    'commission_amount', 'fixed_closing_fee', 'fba_pick_pack', 'technology_fee', 'full_shipping_fee',
    'whf_percent_on_shipping', 'shipping_fee_charged', 'total_charges', 'final_value_after_charges',
    'old_daily_sp', 'old_deal_sp', 'sett_acc_panel', 'net_profit_on_sp', 'net_profit_percent_on_sp',
    'net_profit_percent_on_tp'
]

def safe_float(value):
    try:
        result = float(value or 0)
        if math.isinf(result) or math.isnan(result):
            return 0.0
        return result
    except (TypeError, ValueError):
        return 0.0

def safe_int(value):
    return int(safe_float(value))

def display_value(value):
    if value is None:
        return ""
    return str(value)

def parse_json_dict(value):
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}

def apply_search_and_filters(items, search="", filters=None, skip_column=None):
    filtered = items
    if search:
        q = search.lower()
        filtered = [
            x for x in filtered
            if q in str(x.get('master_sku', '')).lower()
            or q in str(x.get('item_name', '')).lower()
            or q in str(x.get('remark', '')).lower()
            or q in str(x.get('amazon_cat', '')).lower()
        ]

    filters = filters or {}
    for col, values in filters.items():
        if col == skip_column or col not in ALL_COLUMNS:
            continue
        if not isinstance(values, list) or not values:
            continue
        allowed = {display_value(v) for v in values}
        filtered = [x for x in filtered if display_value(x.get(col)) in allowed]

    return filtered

def load_amazon_pricing_db() -> List[Dict[str, Any]]:
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT apr.*, si.item_name 
            FROM amazon_pricing_results apr 
            LEFT JOIN stock_items si ON apr.master_sku = si.sku_code
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{**row, "id": i + 1} for i, row in enumerate(rows)]
    except Exception as e:
        print(f"Error loading amazon pricing from DB: {e}")
        return []

@router.get("/columns")
async def get_columns(user=Depends(get_current_user)):
    return {"visible": DEFAULT_VISIBLE_COLUMNS, "all": ALL_COLUMNS, "editable": [], "labels": COLUMN_LABELS}

@router.get("/filters")
async def get_filters(user=Depends(get_current_user)):
    return {"categories": []}

@router.get("/filter-options")
async def get_filter_options(
    column: str = Query(""),
    search: str = Query(""),
    filters: str = Query(""),
    user=Depends(get_current_user)
):
    if column not in ALL_COLUMNS:
        return {"column": column, "values": []}

    items = load_amazon_pricing_db()
    active_filters = parse_json_dict(filters)
    items = apply_search_and_filters(items, search=search, filters=active_filters, skip_column=column)
    values = sorted({display_value(row.get(column)) for row in items}, key=lambda value: value.lower())
    return {"column": column, "values": values}

@router.get("/")
async def get_amazon_pricing(
    page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500),
    search: str = Query(""), category: str = Query(""),
    sort_by: str = Query("id"), sort_dir: str = Query("asc"),
    filters: str = Query(""),
    user=Depends(get_current_user)
):
    # If role specific access is required, you can implement check_page_access here.
    
    items = load_amazon_pricing_db()
    active_filters = parse_json_dict(filters)
    items = apply_search_and_filters(items, search=search, filters=active_filters)

    if category:
        items = [x for x in items if category.lower() in str(x.get('amazon_cat', '')).lower()]

    stats = {
        "total_skus": len(items),
        "total_available": sum(safe_int(x.get('uniware')) for x in items),
        "total_stock": sum(safe_int(x.get('uniware')) for x in items),
        "total_fba": sum(safe_int(x.get('fba')) for x in items),
        "total_sjit": sum(safe_int(x.get('sjit')) for x in items),
        "total_fbf": sum(safe_int(x.get('fbf')) for x in items)
    }
    
    if sort_by and sort_dir:
        reverse = sort_dir == 'desc'
        if sort_by in NUMERIC_COLUMNS:
            items.sort(key=lambda x: float(x.get(sort_by, 0) or 0), reverse=reverse)
        else:
            items.sort(key=lambda x: (str(x.get(sort_by, '')) or '').lower(), reverse=reverse)
    
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    
    return {"items": page_items, "total": total, "page": page, "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total else 1, "stats": stats}
