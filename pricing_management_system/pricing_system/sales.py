"""Amazon Sales Analytics API — 95 business questions, 11 dimensions.

Uses the same refined logic as the upgraded AmazonSalesAnalytics.py:
- Shipments only (Transaction_Type = 'Shipment') for revenue KPIs
- Refunds = 'Refund' | 'EInvoiceCancel' (negative amounts)
- Cancels = 'Cancel'
- Net revenue = shipments + refunds (refunds are negative)
"""

from fastapi import APIRouter, Depends, Request
from typing import Any
import re
import json
from auth import get_current_user
from database import get_database

router = APIRouter()

WH_SHIP = "WHERE Transaction_Type = 'Shipment'"
WH_REFUND = "WHERE Transaction_Type IN ('Refund','EInvoiceCancel')"
WH_CANCEL = "WHERE Transaction_Type IN ('Cancel')"

SALES_COLUMNS = [
    "Seller_Gstin", "Invoice_Number", "Invoice_Date", "Transaction_Type",
    "Order_Id", "Quantity", "Item_Description", "Asin", "Hsn/sac", "Sku",
    "Ship_From_State", "Ship_To_City", "Ship_To_State", "Ship_To_Postal_Code",
    "Invoice_Amount", "Tax_Exclusive_Gross", "Total_Tax_Amount",
    "Warehouse_Id", "Fulfillment_Channel", "Order_Date",
]


def _clean_filter_value(value: str | None) -> str:
    value = str(value or "").strip()
    return "" if value.lower() in {"", "all", "null", "none"} else value


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _sql_date(value: str | None) -> str:
    value = _clean_filter_value(value)
    return value if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) else ""


def get_sales_filters(request: Request) -> dict[str, str]:
    qp = request.query_params
    source = _clean_filter_value(qp.get("source")) or _clean_filter_value(qp.get("portal")) or "b2b"
    if source.lower() in {"all", "both"}:
        source = "both"
    elif source.lower() not in {"b2b", "b2c"}:
        source = "b2b"
    else:
        source = source.lower()
    return {
        "source": source,
        "start_date": _sql_date(qp.get("start_date")),
        "end_date": _sql_date(qp.get("end_date")),
        "account": _clean_filter_value(qp.get("account")),
        "fulfillment": _clean_filter_value(qp.get("fulfillment")),
        "category": _clean_filter_value(qp.get("category")),
        "style_id": _clean_filter_value(qp.get("style_id")),
        "style_status": _clean_filter_value(qp.get("style_status")),
        "size": _clean_filter_value(qp.get("size")),
    }


def _has_global_filters(filters: dict[str, str] | None) -> bool:
    if not filters:
        return False
    return any(filters.get(k) for k in (
        "start_date", "end_date", "account", "fulfillment",
        "category", "style_id", "style_status", "size",
    ))


def _has_item_filters(filters: dict[str, str] | None) -> bool:
    if not filters:
        return False
    return any(filters.get(k) for k in ("category", "style_id", "style_status", "size"))


def _sales_filter_conditions(filters: dict[str, str] | None, sales_alias: str = "base", item_alias: str = "im") -> list[str]:
    if not filters:
        return []
    conditions = []
    if filters.get("start_date"):
        conditions.append(f"{sales_alias}.Invoice_Date >= {_sql_string(filters['start_date'])}")
    if filters.get("end_date"):
        conditions.append(f"{sales_alias}.Invoice_Date < DATE_ADD({_sql_string(filters['end_date'])}, INTERVAL 1 DAY)")
    if filters.get("account"):
        conditions.append(f"{sales_alias}.Seller_Gstin = {_sql_string(filters['account'])}")
    if filters.get("fulfillment"):
        conditions.append(f"{sales_alias}.Fulfillment_Channel = {_sql_string(filters['fulfillment'])}")
    if filters.get("category"):
        conditions.append(f"{item_alias}.category = {_sql_string(filters['category'])}")
    if filters.get("style_id"):
        conditions.append(f"{item_alias}.item_name = {_sql_string(filters['style_id'])}")
    if filters.get("style_status"):
        conditions.append(f"{item_alias}.item_type = {_sql_string(filters['style_status'])}")
    if filters.get("size"):
        conditions.append(f"{item_alias}.size = {_sql_string(filters['size'])}")
    return conditions


def _select_source(label: str, table: str, filters: dict[str, str] | None = None) -> str:
    cols = ", ".join(f"base.`{c}`" for c in SALES_COLUMNS)
    join_sql = ""
    if _has_item_filters(filters):
        join_sql = " LEFT JOIN stock_items im ON base.Sku = im.sku_code"
    conditions = _sales_filter_conditions(filters)
    where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
    return f"SELECT '{label}' AS sales_source, {cols} FROM {table} base{join_sql}{where_sql}"


def _sales_table(source: str = "both", alias: str = "sales_data", filters: dict[str, str] | None = None) -> str:
    source = (source or "both").lower()
    if _has_global_filters(filters):
        if source == "b2b":
            return f"({_select_source('B2B', 'amazon_sales_b2b', filters)}) {alias}"
        if source == "b2c":
            return f"({_select_source('B2C', 'amazon_sales_b2c', filters)}) {alias}"
        return f"({_select_source('B2B', 'amazon_sales_b2b', filters)} UNION ALL {_select_source('B2C', 'amazon_sales_b2c', filters)}) {alias}"
    if source == "b2b":
        return f"amazon_sales_b2b {alias}"
    if source == "b2c":
        return f"amazon_sales_b2c {alias}"
    return f"({_select_source('B2B', 'amazon_sales_b2b')} UNION ALL {_select_source('B2C', 'amazon_sales_b2c')}) {alias}"


def _order_count(source: str = "both", alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    if (source or "both").lower() != "both":
        return f"COUNT(DISTINCT {prefix}Order_Id)"
    return f"COUNT(DISTINCT {prefix}sales_source, {prefix}Order_Id)"


def _same_source_condition(source: str, left_alias: str, right_alias: str) -> str:
    if (source or "both").lower() != "both":
        return "1=1"
    return f"{left_alias}.sales_source = {right_alias}.sales_source"

def _source_label_expr(source: str, alias: str = "s") -> str:
    source = (source or "both").lower()
    if source == "both":
        return f"{alias}.sales_source"
    return "'B2B'" if source == "b2b" else "'B2C'"

def _net_sales_order_key(source: str, alias: str = "s") -> str:
    if (source or "both").lower() == "both":
        return f"CONCAT({alias}.sales_source, ':', {alias}.Order_Id)"
    return f"{alias}.Order_Id"

def _net_sales_metrics(source: str, alias: str = "s") -> str:
    order_key = _net_sales_order_key(source, alias)
    return f"""
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type = 'Shipment' THEN {alias}.Quantity ELSE 0 END),0) AS sales_qty,
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type IN ('Refund','EInvoiceCancel') THEN ABS({alias}.Quantity) ELSE 0 END),0) AS return_qty,
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type = 'Shipment' THEN {alias}.Quantity ELSE 0 END),0)
            - COALESCE(SUM(CASE WHEN {alias}.Transaction_Type IN ('Refund','EInvoiceCancel') THEN ABS({alias}.Quantity) ELSE 0 END),0) AS net_qty,
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type = 'Shipment' THEN {alias}.Invoice_Amount ELSE 0 END),0) AS sales_value,
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type IN ('Refund','EInvoiceCancel') THEN ABS({alias}.Invoice_Amount) ELSE 0 END),0) AS return_value,
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type = 'Shipment' THEN {alias}.Invoice_Amount ELSE 0 END),0)
            - COALESCE(SUM(CASE WHEN {alias}.Transaction_Type IN ('Refund','EInvoiceCancel') THEN ABS({alias}.Invoice_Amount) ELSE 0 END),0) AS net_sales_value,
        COALESCE(SUM(CASE WHEN {alias}.Transaction_Type = 'Shipment' THEN {alias}.Total_Tax_Amount ELSE 0 END),0) AS sales_tax,
        COUNT(DISTINCT CASE WHEN {alias}.Transaction_Type = 'Shipment' THEN {order_key} END) AS sales_orders,
        COUNT(DISTINCT CASE WHEN {alias}.Transaction_Type IN ('Refund','EInvoiceCancel') THEN {order_key} END) AS return_orders
    """

def _format_net_sales_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    money_keys = {"sales_value", "return_value", "net_sales_value", "sales_tax", "asp"}
    qty_keys = {"sales_qty", "return_qty", "net_qty"}
    pct_keys = {"return_qty_pct", "return_value_pct"}
    for key in money_keys | qty_keys | pct_keys:
        if key in out:
            out[key] = _fmt(out[key])
    for key in ("sales_orders", "return_orders"):
        if key in out:
            out[key] = int(out[key] or 0)
    return out

def _with_net_sales_ratios(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    formatted = []
    for row in rows:
        sales_qty = _val(row.get("sales_qty"))
        sales_value = _val(row.get("sales_value"))
        return_qty = _val(row.get("return_qty"))
        return_value = _val(row.get("return_value"))
        net_qty = _val(row.get("net_qty"))
        net_sales_value = _val(row.get("net_sales_value"))
        row["return_qty_pct"] = return_qty / sales_qty * 100 if sales_qty > 0 else 0
        row["return_value_pct"] = return_value / sales_value * 100 if sales_value > 0 else 0
        row["asp"] = net_sales_value / net_qty if net_qty > 0 else 0
        formatted.append(_format_net_sales_row(row))
    return formatted

def _launch_date_expr(alias: str = "im") -> str:
    raw = f"TRIM({alias}.updated)"
    return f"""
        CASE
            WHEN {raw} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN STR_TO_DATE(LEFT({raw}, 10), '%Y-%m-%d')
            WHEN {raw} REGEXP '^[0-9]{{1,2}}-[A-Za-z]{{3}}-[0-9]{{2}}$' THEN STR_TO_DATE({raw}, '%d-%b-%y')
            WHEN {raw} REGEXP '^[0-9]{{1,2}}-[A-Za-z]{{3}}-[0-9]{{4}}$' THEN STR_TO_DATE({raw}, '%d-%b-%Y')
            WHEN {raw} REGEXP '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' THEN STR_TO_DATE({raw}, '%d/%m/%Y')
            ELSE NULL
        END
    """

def _val(v, default=0):
    return float(v) if v is not None else default

def _fmt(v):
    return round(float(v), 2) if v is not None else 0

def _fetch(sql: str) -> list[dict[str, Any]]:
    try:
        return list(get_database().FetchAll(sql))
    except Exception as e:
        print(f"Sales DB error: {e}")
        return []

def _fetch_one(sql: str) -> dict[str, Any] | None:
    rows = _fetch(sql)
    return rows[0] if rows else None

def _fetch_val(sql: str, default=0):
    row = _fetch_one(sql)
    if row is None:
        return default
    val = list(row.values())[0]
    return float(val) if val is not None else default

def _get(d: dict[str, Any] | None, key: str, default=0):
    if d is None:
        return default
    v = d.get(key)
    return float(v) if v is not None else default


# ════════════════════════════════════════════════════════════════════════════════
# 1. REVENUE & SALES PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════

def _option_values(sql: str, key: str, limit: int = 500) -> list[str]:
    values = []
    for row in _fetch(sql):
        value = row.get(key)
        if value not in (None, ""):
            values.append(str(value))
        if len(values) >= limit:
            break
    return values


@router.get("/filter-options")
async def get_filter_options(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    return {
        "accounts": _option_values(f"""
            SELECT DISTINCT Seller_Gstin AS value FROM {sales}
            WHERE Seller_Gstin IS NOT NULL AND Seller_Gstin != ''
            ORDER BY value LIMIT 200
        """, "value", 200),
        "fulfillment": _option_values(f"""
            SELECT DISTINCT Fulfillment_Channel AS value FROM {sales}
            WHERE Fulfillment_Channel IS NOT NULL AND Fulfillment_Channel != ''
            ORDER BY value LIMIT 100
        """, "value", 100),
        "categories": _option_values("""
            SELECT DISTINCT category AS value FROM stock_items
            WHERE category IS NOT NULL AND category != ''
            ORDER BY value LIMIT 500
        """, "value", 500),
        "style_ids": _option_values("""
            SELECT DISTINCT item_name AS value FROM stock_items
            WHERE item_name IS NOT NULL AND item_name != ''
            ORDER BY value LIMIT 1000
        """, "value", 1000),
        "style_statuses": _option_values("""
            SELECT DISTINCT item_type AS value FROM stock_items
            WHERE item_type IS NOT NULL AND item_type != ''
            ORDER BY value LIMIT 100
        """, "value", 100),
        "sizes": _option_values("""
            SELECT DISTINCT size AS value FROM stock_items
            WHERE size IS NOT NULL AND size != ''
            ORDER BY value LIMIT 200
        """, "value", 200),
    }


@router.get("/summary")
async def get_summary(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Top-level KPIs: gross/net revenue, refunds, AOV, ASP, tax."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    ship = _fetch_one(f"""
        SELECT COALESCE(SUM(Invoice_Amount),0) AS gross_revenue,
               {_order_count(source)} AS shipped_orders,
               COALESCE(SUM(Quantity),0) AS total_units,
               COALESCE(SUM(Total_Tax_Amount),0) AS total_tax,
               COALESCE(SUM(Tax_Exclusive_Gross),0) AS gross_excl
        FROM {sales} {WH_SHIP}
    """)
    refund_sum = abs(_fetch_val(f"SELECT COALESCE(SUM(Invoice_Amount),0) FROM {sales} {WH_REFUND}"))
    cancel_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales} {WH_CANCEL}")
    total_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales}")
    total_rows = _fetch_val(f"SELECT COUNT(*) FROM {sales}")
    refund_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales} {WH_REFUND}")

    gr = _get(ship, "gross_revenue")
    so = int(_get(ship, "shipped_orders"))
    tu = int(_get(ship, "total_units"))
    tt = _get(ship, "total_tax")
    ge = _get(ship, "gross_excl")
    nr = gr - refund_sum

    return {
        "gross_revenue": _fmt(gr), "refund_amount": _fmt(refund_sum),
        "net_revenue": _fmt(nr),
        "gross_to_net_gap_pct": _fmt(refund_sum / gr * 100) if gr > 0 else 0,
        "total_orders": int(total_orders), "shipped_orders": so,
        "refund_orders": int(refund_orders), "cancel_orders": int(cancel_orders),
        "total_units": tu, "total_tax": _fmt(tt), "gross_excl": _fmt(ge),
        "aov": _fmt(gr / so) if so > 0 else 0,
        "asp": _fmt(gr / tu) if tu > 0 else 0,
        "total_rows": int(total_rows),
        "refund_rate": _fmt(refund_orders / total_orders * 100) if total_orders > 0 else 0,
        "cancel_rate": _fmt(cancel_orders / total_orders * 100) if total_orders > 0 else 0,
    }


@router.get("/sales-performance")
async def get_sales_performance(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Revenue trends, channel rev, GSTIN rev, concentration."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    monthly = _fetch(f"""
        SELECT DATE_FORMAT(Invoice_Date, '%Y-%m-01') AS month,
               COALESCE(SUM(Invoice_Amount),0) AS revenue
        FROM {sales} {WH_SHIP}
        GROUP BY DATE_FORMAT(Invoice_Date, '%Y-%m-01') ORDER BY month
    """)
    channel_rev = _fetch(f"""
        SELECT Fulfillment_Channel AS channel,
               COALESCE(SUM(Invoice_Amount),0) AS revenue,
               {_order_count(source)} AS orders,
               COALESCE(SUM(Quantity),0) AS units
        FROM {sales} {WH_SHIP}
        AND Fulfillment_Channel IS NOT NULL AND Fulfillment_Channel != ''
        GROUP BY channel ORDER BY revenue DESC
    """)
    gstin_rev = _fetch(f"""
        SELECT Seller_Gstin AS gstin,
               COALESCE(SUM(Invoice_Amount),0) AS revenue,
               {_order_count(source)} AS orders
        FROM {sales} {WH_SHIP}
        AND Seller_Gstin IS NOT NULL AND Seller_Gstin != ''
        GROUP BY gstin ORDER BY revenue DESC
    """)
    total_gr = _fetch_val(f"SELECT COALESCE(SUM(Invoice_Amount),0) FROM {sales} {WH_SHIP}")
    top10 = _fetch(f"""
        SELECT Order_Id, COALESCE(SUM(Invoice_Amount),0) AS order_revenue
        FROM {sales} {WH_SHIP}
        GROUP BY Order_Id ORDER BY order_revenue DESC LIMIT 10
    """)
    top10_sum = float(sum(r["order_revenue"] for r in top10))
    conc_pct = _fmt(top10_sum / total_gr * 100) if total_gr > 0 else 0

    growth = []
    prev = None
    for row in monthly:
        r = float(row["revenue"])
        g = None
        if prev is not None and prev > 0:
            g = _fmt((r - prev) / prev * 100)
        growth.append({"month": row["month"], "revenue": _fmt(r), "growth": g})
        prev = r

    return {
        "monthly": growth,
        "channel_rev": [{**c, "revenue": _fmt(c["revenue"]), "orders": int(c["orders"]), "units": int(c["units"])} for c in channel_rev],
        "gstin_rev": [{**g, "revenue": _fmt(g["revenue"]), "orders": int(g["orders"])} for g in gstin_rev],
        "top10_concentration_pct": conc_pct,
    }


# ════════════════════════════════════════════════════════════════════════════════
# 2. TRANSACTION QUALITY & HEALTH
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/net-sales-dashboard")
async def get_net_sales_dashboard(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Net sales, returns, and style-level performance dashboard."""
    source = filters["source"]
    sales = _sales_table(source, "s", filters)
    source_expr = _source_label_expr(source, "s")
    metrics = _net_sales_metrics(source, "s")
    item_join = "LEFT JOIN stock_items im ON s.Sku = im.sku_code"
    launch_date = _launch_date_expr("im")

    kpi_row = _fetch_one(f"SELECT {metrics} FROM {sales} {item_join}") or {}
    kpis = _with_net_sales_ratios([kpi_row])[0] if kpi_row else {}
    kpis.update({
        "rto_qty": None,
        "customer_return_qty": None,
        "fc_return_qty": None,
        "rto_qty_pct": None,
        "customer_return_pct": None,
        "fc_return_pct": None,
    })

    monthly = _fetch(f"""
        SELECT DATE_FORMAT(s.Invoice_Date, '%Y-%m-01') AS month, {metrics}
        FROM {sales} {item_join}
        WHERE s.Invoice_Date IS NOT NULL
        GROUP BY DATE_FORMAT(s.Invoice_Date, '%Y-%m-01')
        ORDER BY month
    """)
    portal = _fetch(f"""
        SELECT {source_expr} AS portal,
               COALESCE(NULLIF(s.Fulfillment_Channel,''), 'Unmapped') AS fulfillment_type,
               {metrics}
        FROM {sales} {item_join}
        GROUP BY {source_expr}, COALESCE(NULLIF(s.Fulfillment_Channel,''), 'Unmapped')
        ORDER BY net_sales_value DESC
        LIMIT 100
    """)
    region = _fetch(f"""
        SELECT COALESCE(NULLIF(s.Ship_To_State,''), 'Unmapped') AS region,
               COALESCE(NULLIF(s.Ship_To_State,''), 'Unmapped') AS delivered_state,
               {source_expr} AS portal,
               {metrics}
        FROM {sales} {item_join}
        GROUP BY COALESCE(NULLIF(s.Ship_To_State,''), 'Unmapped'), {source_expr}
        ORDER BY net_sales_value DESC
        LIMIT 100
    """)
    style = _fetch(f"""
        SELECT COALESCE(NULLIF(im.category,''), 'Unmapped') AS category,
               COALESCE(NULLIF(im.item_name,''), s.Sku, 'Unmapped') AS style_id,
               COALESCE(NULLIF(im.item_type,''), 'Unmapped') AS style_status,
               COALESCE(NULLIF(im.size,''), 'Unmapped') AS size,
               {metrics}
        FROM {sales} {item_join}
        GROUP BY COALESCE(NULLIF(im.category,''), 'Unmapped'),
                 COALESCE(NULLIF(im.item_name,''), s.Sku, 'Unmapped'),
                 COALESCE(NULLIF(im.item_type,''), 'Unmapped'),
                 COALESCE(NULLIF(im.size,''), 'Unmapped')
        ORDER BY net_sales_value DESC
        LIMIT 100
    """)
    launchdate = _fetch(f"""
        SELECT COALESCE(CAST(YEAR({launch_date}) AS CHAR), 'Unmapped') AS launch_year,
               COALESCE(DATE_FORMAT({launch_date}, '%b'), 'Unmapped') AS launch_month,
               COALESCE(MONTH({launch_date}), 0) AS launch_month_no,
               COALESCE(NULLIF(im.category,''), 'Unmapped') AS category,
               COALESCE(NULLIF(im.item_name,''), s.Sku, 'Unmapped') AS style_id,
               {metrics}
        FROM {sales} {item_join}
        GROUP BY COALESCE(CAST(YEAR({launch_date}) AS CHAR), 'Unmapped'),
                 COALESCE(DATE_FORMAT({launch_date}, '%b'), 'Unmapped'),
                 COALESCE(MONTH({launch_date}), 0),
                 COALESCE(NULLIF(im.category,''), 'Unmapped'),
                 COALESCE(NULLIF(im.item_name,''), s.Sku, 'Unmapped')
        ORDER BY CASE WHEN launch_year = 'Unmapped' THEN 1 ELSE 0 END,
                 launch_year DESC, launch_month_no DESC, net_sales_value DESC
        LIMIT 100
    """)

    return {
        "kpis": kpis,
        "monthly": _with_net_sales_ratios(monthly),
        "portal_table": _with_net_sales_ratios(portal),
        "region_table": _with_net_sales_ratios(region),
        "style_table": _with_net_sales_ratios(style),
        "launchdate_table": _with_net_sales_ratios(launchdate),
    }


@router.get("/transactions")
async def get_transactions(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Refund/cancel rates, same-day refunds, duplicates, return loops."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    total_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales}")
    shipped_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales} {WH_SHIP}")
    refund_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales} {WH_REFUND}")
    cancel_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales} {WH_CANCEL}")
    refund_sum = _fetch_val(f"SELECT COALESCE(SUM(Invoice_Amount),0) FROM {sales} {WH_REFUND}")
    same_day = _fetch_val(f"""
        SELECT {_order_count(source, 'r')}
        FROM {_sales_table(source, 'r', filters)} INNER JOIN {_sales_table(source, 's', filters)} ON r.Order_Id = s.Order_Id AND {_same_source_condition(source, 'r', 's')} AND s.Transaction_Type = 'Shipment'
        WHERE r.Transaction_Type IN ('Refund','EInvoiceCancel') AND DATE(r.Invoice_Date) = DATE(s.Order_Date)
    """)
    dup_inv = _fetch(f"""
        SELECT Invoice_Number, COUNT(*) AS cnt
        FROM {sales}
        WHERE Invoice_Number IS NOT NULL AND Invoice_Number != ''
        GROUP BY Invoice_Number HAVING cnt > 1 ORDER BY cnt DESC LIMIT 10
    """)
    loop_count = _fetch_val(f"""
        SELECT {_order_count(source, 's')}
        FROM {_sales_table(source, 's', filters)} INNER JOIN {_sales_table(source, 'r', filters)} ON s.Order_Id = r.Order_Id AND {_same_source_condition(source, 's', 'r')}
        WHERE s.Transaction_Type = 'Shipment' AND r.Transaction_Type IN ('Refund','EInvoiceCancel')
    """)
    gstin_stats = _fetch(f"""
        SELECT Seller_Gstin,
               SUM(CASE WHEN Transaction_Type = 'Cancel' THEN 1 ELSE 0 END) AS cancels,
               SUM(CASE WHEN Transaction_Type = 'Shipment' THEN 1 ELSE 0 END) AS ships
        FROM {sales} WHERE Seller_Gstin IS NOT NULL AND Seller_Gstin != ''
        GROUP BY Seller_Gstin
    """)
    for g in gstin_stats:
        total = int(g["cancels"]) + int(g["ships"])
        g["cancel_rate"] = _fmt(int(g["cancels"]) / total * 100) if total > 0 else 0
    multi_refund = _fetch_val(f"""
        SELECT COUNT(*) FROM (SELECT Order_Id FROM {sales}
        WHERE Transaction_Type IN ('Refund','EInvoiceCancel')
        GROUP BY Order_Id HAVING COUNT(*) > 1) mr
    """)
    return {
        "total_orders": int(total_orders), "shipped_orders": int(shipped_orders),
        "refund_orders": int(refund_orders), "cancel_orders": int(cancel_orders),
        "net_shipment_rate": _fmt((shipped_orders - refund_orders) / shipped_orders * 100) if shipped_orders > 0 else 0,
        "refund_rate": _fmt(refund_orders / total_orders * 100) if total_orders > 0 else 0,
        "cancel_rate": _fmt(cancel_orders / total_orders * 100) if total_orders > 0 else 0,
        "refund_amount": _fmt(abs(refund_sum)),
        "same_day_refunds": int(same_day), "dup_invoice_count": len(dup_inv),
        "dup_invoices": [{"invoice": r["Invoice_Number"], "count": int(r["cnt"])} for r in dup_inv],
        "return_loop_orders": int(loop_count), "multi_refund_orders": int(multi_refund),
        "gstin_stats": [{**g, "cancels": int(g["cancels"]), "ships": int(g["ships"])} for g in gstin_stats],
    }


# ════════════════════════════════════════════════════════════════════════════════
# 3. PRODUCT & SKU INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/products")
async def get_products(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """SKU-level: net revenue, refund rates, tax rates, Pareto."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    sku_ship = _fetch(f"""
        SELECT s.Sku, MAX(COALESCE(m.Master_SKU, s.Sku)) AS master_sku,
               COALESCE(SUM(s.Invoice_Amount),0) AS gross_revenue,
               COALESCE(SUM(s.Quantity),0) AS units,
               COALESCE(SUM(s.Total_Tax_Amount),0) AS tax,
               {_order_count(source, 's')} AS orders
        FROM {_sales_table(source, 's', filters)} LEFT JOIN amazon_sku_code_mapping m ON s.Sku = m.Master_SKU
        WHERE s.Transaction_Type = 'Shipment' GROUP BY s.Sku
    """)
    sku_refund = _fetch(f"""
        SELECT Sku, COALESCE(SUM(Invoice_Amount),0) AS refund_amount
        FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel') GROUP BY Sku
    """)
    refund_map = {r["Sku"]: abs(float(r["refund_amount"])) for r in sku_refund}
    stats = []
    total_net = 0.0
    for s in sku_ship:
        gr = float(s["gross_revenue"])
        rd = refund_map.get(s["Sku"], 0.0)
        nr = gr - rd; units = int(s["units"]); tax = float(s["tax"])
        total_net += nr
        stats.append({"sku": s["Sku"], "master_sku": s["master_sku"],
            "gross_revenue": _fmt(gr), "refund_amount": _fmt(rd),
            "net_revenue": _fmt(nr), "units": units, "tax": _fmt(tax),
            "orders": int(s["orders"]),
            "refund_rate": _fmt(rd / gr * 100) if gr > 0 else 0,
            "tax_rate": _fmt(tax / gr * 100) if gr > 0 else 0})
    stats.sort(key=lambda x: x["net_revenue"], reverse=True)
    cum = 0.0; pareto = 0
    for s in stats:
        cum += float(s["net_revenue"]); pareto += 1
        if total_net > 0 and cum >= total_net * 0.8: break
    top_asins = _fetch(f"""
        SELECT Asin, COALESCE(SUM(Invoice_Amount),0) AS revenue, {_order_count(source)} AS orders
        FROM {sales} {WH_SHIP} AND Asin IS NOT NULL AND Asin != ''
        GROUP BY Asin ORDER BY revenue DESC LIMIT 10
    """)
    zero_refund = [s for s in stats if float(s["refund_amount"]) == 0][:5]
    high_refund_risk = [s for s in stats if s["refund_rate"] > 30 and s["units"] > 0][:10]
    return {"top_skus": stats[:20],
        "top_asins": [{**a, "revenue": _fmt(a["revenue"]), "orders": int(a["orders"])} for a in top_asins],
        "pareto_count": pareto, "total_net_revenue": _fmt(total_net),
        "zero_refund_skus": zero_refund, "high_refund_risk_skus": high_refund_risk}


# ════════════════════════════════════════════════════════════════════════════════
# 4. CUSTOMER GEOGRAPHY
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/geography")
async def get_geography(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """States (net), cities, postal codes, interstate/intrastate."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    states = _fetch(f"""
        SELECT Ship_To_State AS state, {_order_count(source)} AS orders,
               COALESCE(SUM(Invoice_Amount),0) AS revenue, COALESCE(SUM(Quantity),0) AS units
        FROM {sales} {WH_SHIP} AND Ship_To_State IS NOT NULL AND Ship_To_State != ''
        GROUP BY state ORDER BY orders DESC
    """)
    sr = _fetch(f"""
        SELECT Ship_To_State AS state, COALESCE(SUM(Invoice_Amount),0) AS refund_amt,
               {_order_count(source)} AS refund_orders
        FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel')
        AND Ship_To_State IS NOT NULL AND Ship_To_State != '' GROUP BY state
    """)
    rf_map = {r["state"]: (float(r["refund_amt"]), int(r["refund_orders"])) for r in sr}
    state_net = []
    for s in states:
        st = s["state"]; rd, ro = rf_map.get(st, (0.0, 0)); rev = float(s["revenue"]); ords = int(s["orders"])
        state_net.append({"state": st, "revenue": _fmt(rev), "orders": ords, "units": int(s["units"]),
            "refund_amount": _fmt(abs(rd)), "refund_orders": ro,
            "net_revenue": _fmt(rev - abs(rd)),
            "refund_rate": _fmt(ro / ords * 100) if ords > 0 else 0})
    state_net.sort(key=lambda x: x["net_revenue"], reverse=True)
    cities = _fetch(f"""
        SELECT Ship_To_City AS city, MAX(Ship_To_State) AS state,
               {_order_count(source)} AS orders, COALESCE(SUM(Invoice_Amount),0) AS revenue
        FROM {sales} {WH_SHIP} AND Ship_To_City IS NOT NULL AND Ship_To_City != ''
        GROUP BY Ship_To_City ORDER BY orders DESC LIMIT 10
    """)
    postal = _fetch(f"""
        SELECT Ship_To_Postal_Code AS pincode, COALESCE(SUM(Invoice_Amount),0) AS revenue
        FROM {sales} {WH_SHIP} AND Ship_To_Postal_Code IS NOT NULL AND Ship_To_Postal_Code != ''
        GROUP BY pincode ORDER BY revenue DESC LIMIT 10
    """)
    total_ship_rows = _fetch_val(f"SELECT COUNT(*) FROM {sales} {WH_SHIP}")
    intra = _fetch_val(f"""
        SELECT COUNT(*) FROM {sales} {WH_SHIP}
        AND UPPER(TRIM(Ship_From_State)) = UPPER(TRIM(Ship_To_State))
    """)
    inter = total_ship_rows - intra
    return {"states": state_net,
        "cities": [{**c, "revenue": _fmt(c["revenue"]), "orders": int(c["orders"])} for c in cities],
        "postal_codes": [{**p, "revenue": _fmt(p["revenue"])} for p in postal],
        "interstate_shipments": int(inter), "intrastate_shipments": int(intra),
        "interstate_pct": _fmt(inter / total_ship_rows * 100) if total_ship_rows > 0 else 0,
        "intrastate_pct": _fmt(intra / total_ship_rows * 100) if total_ship_rows > 0 else 0}


# ════════════════════════════════════════════════════════════════════════════════
# 5. SHIPPING & ROUTES
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/shipping")
async def get_shipping(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Origins, routes, high-refund routes."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    origins = _fetch(f"""
        SELECT Ship_From_State AS state, COUNT(*) AS shipments, {_order_count(source)} AS orders
        FROM {sales} {WH_SHIP} AND Ship_From_State IS NOT NULL AND Ship_From_State != ''
        GROUP BY state ORDER BY shipments DESC LIMIT 10
    """)
    routes = _fetch(f"""
        SELECT Ship_From_State AS from_state, Ship_To_State AS to_state,
               {_order_count(source)} AS route_volume, COALESCE(SUM(Invoice_Amount),0) AS revenue
        FROM {sales} {WH_SHIP}
        AND Ship_From_State IS NOT NULL AND Ship_From_State != ''
        AND Ship_To_State IS NOT NULL AND Ship_To_State != ''
        GROUP BY from_state, to_state ORDER BY route_volume DESC LIMIT 10
    """)
    refund_routes = _fetch(f"""
        SELECT Ship_From_State AS from_state, Ship_To_State AS to_state,
               {_order_count(source)} AS refund_count
        FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel')
        AND Ship_From_State IS NOT NULL AND Ship_From_State != ''
        AND Ship_To_State IS NOT NULL AND Ship_To_State != ''
        GROUP BY from_state, to_state ORDER BY refund_count DESC LIMIT 10
    """)
    return {"origins": [{**o, "shipments": int(o["shipments"]), "orders": int(o["orders"])} for o in origins],
        "routes": [{**r, "route_volume": int(r["route_volume"]), "revenue": _fmt(r["revenue"])} for r in routes],
        "refund_routes": [{**rr, "refund_count": int(rr["refund_count"])} for rr in refund_routes]}


# ════════════════════════════════════════════════════════════════════════════════
# 6. WAREHOUSE + FULFILLMENT
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/warehouse")
async def get_warehouse(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Warehouse + fulfillment with refund rates, rev per order."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    wh = _fetch(f"""
        SELECT Warehouse_Id AS warehouse, {_order_count(source)} AS orders,
               COALESCE(SUM(Invoice_Amount),0) AS revenue
        FROM {sales} {WH_SHIP} AND Warehouse_Id IS NOT NULL AND Warehouse_Id != ''
        GROUP BY warehouse ORDER BY orders DESC
    """)
    wh_refund = _fetch(f"""
        SELECT Warehouse_Id AS warehouse, {_order_count(source)} AS refund_orders
        FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel')
        AND Warehouse_Id IS NOT NULL AND Warehouse_Id != '' GROUP BY warehouse
    """)
    rf_map = {r["warehouse"]: int(r["refund_orders"]) for r in wh_refund}
    wh_stats = []
    for w in wh:
        ro = rf_map.get(w["warehouse"], 0); rev = float(w["revenue"]); ords = int(w["orders"])
        wh_stats.append({"warehouse": w["warehouse"], "orders": ords, "revenue": _fmt(rev),
            "refund_orders": ro, "refund_rate": _fmt(ro / ords * 100) if ords > 0 else 0,
            "rev_per_order": _fmt(rev / ords) if ords > 0 else 0})
    fc = _fetch(f"""
        SELECT Fulfillment_Channel AS channel, {_order_count(source)} AS orders,
               COALESCE(SUM(Invoice_Amount),0) AS gross_revenue, COALESCE(SUM(Quantity),0) AS units
        FROM {sales} {WH_SHIP} AND Fulfillment_Channel IS NOT NULL AND Fulfillment_Channel != ''
        GROUP BY channel ORDER BY gross_revenue DESC
    """)
    fc_refund = _fetch(f"""
        SELECT Fulfillment_Channel AS channel, {_order_count(source)} AS refund_orders,
               COALESCE(SUM(Invoice_Amount),0) AS refund_amt
        FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel')
        AND Fulfillment_Channel IS NOT NULL AND Fulfillment_Channel != '' GROUP BY channel
    """)
    fc_rf_map = {f["channel"]: (int(f["refund_orders"]), float(f["refund_amt"])) for f in fc_refund}
    fc_stats = []
    for f in fc:
        ro, ra = fc_rf_map.get(f["channel"], (0, 0.0)); gr = float(f["gross_revenue"]); ords = int(f["orders"])
        fc_stats.append({"channel": f["channel"], "orders": ords, "gross_revenue": _fmt(gr),
            "refund_orders": ro, "refund_amount": _fmt(abs(ra)),
            "net_revenue": _fmt(gr - abs(ra)), "units": int(f["units"]),
            "refund_rate": _fmt(ro / ords * 100) if ords > 0 else 0,
            "aov": _fmt(gr / ords) if ords > 0 else 0})
    return {"warehouses": wh_stats, "fulfillment": fc_stats}


# ════════════════════════════════════════════════════════════════════════════════
# 7. TAX & COMPLIANCE
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/tax")
async def get_tax(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Tax totals, effective rate, by state/HSN, IGST/CGST split."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    ship = _fetch_one(f"""
        SELECT COALESCE(SUM(Total_Tax_Amount),0) AS total_tax,
               COALESCE(SUM(Tax_Exclusive_Gross),0) AS gross_excl
        FROM {sales} {WH_SHIP}
    """)
    tt = _get(ship, "total_tax"); ge = _get(ship, "gross_excl")
    tax_by_state = _fetch(f"""
        SELECT Ship_To_State AS state, COALESCE(SUM(Total_Tax_Amount),0) AS tax
        FROM {sales} {WH_SHIP} AND Ship_To_State IS NOT NULL AND Ship_To_State != ''
        GROUP BY state ORDER BY tax DESC LIMIT 10
    """)
    tax_by_hsn = _fetch(f"""
        SELECT `Hsn/sac` AS hsn, COALESCE(SUM(Total_Tax_Amount),0) AS tax
        FROM {sales} {WH_SHIP}
        AND `Hsn/sac` IS NOT NULL AND `Hsn/sac` != ''
        GROUP BY hsn ORDER BY tax DESC LIMIT 10
    """)
    missing_hsn = _fetch_val(f"""
        SELECT COUNT(*) FROM {sales} {WH_SHIP}
        AND (`Hsn/sac` IS NULL OR TRIM(`Hsn/sac`) = '')
    """)
    igst = _fetch_val(f"""
        SELECT COALESCE(SUM(Total_Tax_Amount),0) FROM {sales} {WH_SHIP}
        AND UPPER(TRIM(Ship_From_State)) != UPPER(TRIM(Ship_To_State))
    """)
    cgst = _fetch_val(f"""
        SELECT COALESCE(SUM(Total_Tax_Amount),0) FROM {sales} {WH_SHIP}
        AND UPPER(TRIM(Ship_From_State)) = UPPER(TRIM(Ship_To_State))
    """)
    return {"total_tax": _fmt(tt), "effective_tax_rate": _fmt(tt / ge * 100) if ge > 0 else 0,
        "tax_by_state": [{**t, "tax": _fmt(t["tax"])} for t in tax_by_state],
        "tax_by_hsn": [{**h, "tax": _fmt(h["tax"])} for h in tax_by_hsn],
        "missing_hsn_rows": int(missing_hsn), "igst_tax": _fmt(igst), "cgst_sgst_tax": _fmt(cgst)}


# ════════════════════════════════════════════════════════════════════════════════
# 8. DATA QUALITY
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/data-quality")
async def get_data_quality(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Duplicate invoices, missing data, zero amounts, orphans."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    return {
        "missing_invoice_rows": int(_fetch_val(f"SELECT COUNT(*) FROM {sales} WHERE Invoice_Number IS NULL OR Invoice_Number = ''")),
        "missing_warehouse_rows": int(_fetch_val(f"SELECT COUNT(*) FROM {sales} WHERE Warehouse_Id IS NULL OR Warehouse_Id = ''")),
        "zero_amount_shipments": int(_fetch_val(f"SELECT COUNT(*) FROM {sales} {WH_SHIP} AND Invoice_Amount = 0")),
        "negative_non_refund_rows": int(_fetch_val(f"SELECT COUNT(*) FROM {sales} WHERE Transaction_Type NOT IN ('Refund','EInvoiceCancel','Cancel') AND Invoice_Amount < 0")),
        "avg_qty_per_order": _fmt(_fetch_val(f"SELECT COALESCE(SUM(Quantity),0) / NULLIF({_order_count(source)},0) FROM {sales} {WH_SHIP}")),
        "orphan_refund_orders": int(_fetch_val(f"SELECT {_order_count(source, 'r')} FROM {_sales_table(source, 'r', filters)} WHERE r.Transaction_Type IN ('Refund','EInvoiceCancel') AND NOT EXISTS (SELECT 1 FROM {_sales_table(source, 's', filters)} WHERE s.Order_Id = r.Order_Id AND ({_same_source_condition(source, 's', 'r')}) AND s.Transaction_Type = 'Shipment')")),
        "dup_invoice_count": int(_fetch_val(f"SELECT COUNT(*) FROM (SELECT Invoice_Number FROM {sales} WHERE Invoice_Number IS NOT NULL AND Invoice_Number != '' GROUP BY Invoice_Number HAVING COUNT(*) > 1) d")),
    }


# ════════════════════════════════════════════════════════════════════════════════
# 9. OPERATIONAL INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/operations")
async def get_operations(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Restock candidates, stockout signals."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    sku_ship = _fetch(f"""
        SELECT s.Sku, MAX(COALESCE(m.Master_SKU, s.Sku)) AS master_sku,
               COALESCE(SUM(s.Invoice_Amount),0) AS gross_revenue,
               COALESCE(SUM(s.Quantity),0) AS units
        FROM {_sales_table(source, 's', filters)} LEFT JOIN amazon_sku_code_mapping m ON s.Sku = m.Master_SKU
        WHERE s.Transaction_Type = 'Shipment' GROUP BY s.Sku
    """)
    refund_map = {r["Sku"]: abs(float(r["refund_amount"])) for r in
        _fetch(f"SELECT Sku, COALESCE(SUM(Invoice_Amount),0) AS refund_amount FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel') GROUP BY Sku")}
    mean_units = _fetch_val(f"SELECT COALESCE(AVG(uq),0) FROM (SELECT COALESCE(SUM(Quantity),0) AS uq FROM {sales} {WH_SHIP} GROUP BY Sku) sq")
    candidates = []; signals = []
    for s in sku_ship:
        gr = float(s["gross_revenue"]); rd = refund_map.get(s["Sku"], 0.0); nr = gr - rd; units = int(s["units"]); rr = rd / gr * 100 if gr > 0 else 0
        if units > mean_units and rr < 10:
            candidates.append({"sku": s["Sku"], "master_sku": s["master_sku"], "units": units, "net_revenue": _fmt(nr), "refund_rate": _fmt(rr)})
        if units > 0 and units < mean_units * 0.25 and rd == 0:
            signals.append({"sku": s["Sku"], "master_sku": s["master_sku"], "units": units, "net_revenue": _fmt(nr)})
    candidates.sort(key=lambda x: x["units"], reverse=True); signals.sort(key=lambda x: x["units"])
    return {"restock_candidates": candidates[:10], "stockout_signals": signals[:10]}


# ════════════════════════════════════════════════════════════════════════════════
# 10. EXECUTIVE SUMMARY
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/executive")
async def get_executive(filters: dict[str, str] = Depends(get_sales_filters), user=Depends(get_current_user)):
    """Business health snapshot."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)
    ship = _fetch_one(f"""
        SELECT COALESCE(SUM(Invoice_Amount),0) AS gross_revenue,
               {_order_count(source)} AS shipped_orders,
               COALESCE(SUM(Quantity),0) AS total_units,
               COALESCE(SUM(Total_Tax_Amount),0) AS total_tax,
               COALESCE(SUM(Tax_Exclusive_Gross),0) AS gross_excl
        FROM {sales} {WH_SHIP}
    """)
    gr = _get(ship, "gross_revenue"); tt = _get(ship, "total_tax"); ge = _get(ship, "gross_excl")
    so = int(_get(ship, "shipped_orders"))
    refund_sum = abs(_fetch_val(f"SELECT COALESCE(SUM(Invoice_Amount),0) FROM {sales} {WH_REFUND}"))
    total_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales}")
    refund_orders = _fetch_val(f"SELECT {_order_count(source)} FROM {sales} {WH_REFUND}")
    nr = gr - refund_sum
    top_sku = _fetch_one(f"SELECT Sku, COALESCE(SUM(Invoice_Amount),0) AS v FROM {sales} {WH_SHIP} GROUP BY Sku ORDER BY v DESC LIMIT 1")
    top_state = _fetch_one(f"SELECT Ship_To_State AS state, COALESCE(SUM(Invoice_Amount),0) AS v FROM {sales} {WH_SHIP} AND Ship_To_State IS NOT NULL AND Ship_To_State != '' GROUP BY state ORDER BY v DESC LIMIT 1")
    top_ch = _fetch_one(f"SELECT Fulfillment_Channel AS channel, COALESCE(SUM(Invoice_Amount),0) AS v FROM {sales} {WH_SHIP} AND Fulfillment_Channel IS NOT NULL AND Fulfillment_Channel != '' GROUP BY channel ORDER BY v DESC LIMIT 1")
    skus = _fetch(f"SELECT s.Sku, COALESCE(SUM(s.Invoice_Amount),0) AS revenue FROM {_sales_table(source, 's', filters)} LEFT JOIN amazon_sku_code_mapping m ON s.Sku = m.Master_SKU WHERE s.Transaction_Type = 'Shipment' GROUP BY s.Sku ORDER BY revenue DESC")
    sku_ref_map = {r["Sku"]: abs(float(r["refund_amount"])) for r in
        _fetch(f"SELECT Sku, COALESCE(SUM(Invoice_Amount),0) AS refund_amount FROM {sales} WHERE Transaction_Type IN ('Refund','EInvoiceCancel') GROUP BY Sku")}
    tn = sum(float(s["revenue"]) - sku_ref_map.get(s["Sku"], 0.0) for s in skus)
    cum = 0.0; pareto = 0
    for s in skus:
        cum += float(s["revenue"]) - sku_ref_map.get(s["Sku"], 0.0); pareto += 1
        if tn > 0 and cum >= tn * 0.8: break
    return {"gross_revenue": _fmt(gr), "net_revenue": _fmt(nr), "refund_amount": _fmt(refund_sum),
        "refund_rate": _fmt(refund_orders / total_orders * 100) if total_orders > 0 else 0,
        "net_shipment_rate": _fmt((so - refund_orders) / so * 100) if so > 0 else 0,
        "total_tax": _fmt(tt), "effective_tax_rate": _fmt(tt / ge * 100) if ge > 0 else 0,
        "top_sku": top_sku["Sku"] if top_sku else None,
        "top_sku_revenue": _fmt(_get(top_sku, "v")),
        "top_state": top_state["state"] if top_state else None,
        "top_channel": top_ch["channel"] if top_ch else None, "pareto_count": pareto}


# ════════════════════════════════════════════════════════════════════════════════
# 11. DATA TABLE (RAW DATA WITH PAGINATION)
# ════════════════════════════════════════════════════════════════════════════════

from fastapi.responses import StreamingResponse
import io
import csv


@router.get("/data-table")
async def get_data_table(
    request: Request,
    filters: dict[str, str] = Depends(get_sales_filters),
    user=Depends(get_current_user)
):
    """Raw sales data with pagination, sorting, filtering, and optional CSV export."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)

    qp = request.query_params
    transaction_type = _clean_filter_value(qp.get("transaction_type"))
    page = max(1, int(qp.get("page", "1") or "1"))
    page_size = min(200, max(1, int(qp.get("page_size", "50") or "50")))
    export = _clean_filter_value(qp.get("export")) == "1"
    sort_by = _clean_filter_value(qp.get("sort_by")) or "Invoice_Date"
    sort_dir = _clean_filter_value(qp.get("sort_dir")) or "desc"

    VALID_SORT_COLS = ["Seller_Gstin", "Invoice_Number", "Invoice_Date", "Transaction_Type",
                       "Order_Id", "Sku", "Master_SKU", "Category", "Size", "Asin", "Quantity", "Invoice_Amount",
                       "Total_Tax_Amount", "Fulfillment_Channel", "Warehouse_Id",
                       "Ship_From_State", "Ship_To_State", "Ship_To_City"]
    if sort_by not in VALID_SORT_COLS:
        sort_by = "Invoice_Date"
    if sort_dir.lower() not in ("asc", "desc"):
        sort_dir = "desc"

    conditions = []
    if transaction_type and transaction_type.lower() != "all":
        tt = transaction_type
        if tt == "Refund":
            conditions.append("Transaction_Type IN ('Refund','EInvoiceCancel')")
        else:
            conditions.append(f"Transaction_Type = {_sql_string(tt)}")

    for key in qp.keys():
        if key.startswith("filter_"):
            col = key.replace("filter_", "", 1)
            if col in VALID_SORT_COLS:
                try:
                    vals = json.loads(qp.get(key))
                    if vals and isinstance(vals, list) and len(vals) > 0:
                        if vals == ["__NO_MATCH__"]:
                            conditions.append("1=0")
                        else:
                            escaped = [_sql_string(str(v)) for v in vals]
                            conditions.append(f"`{col}` IN ({', '.join(escaped)})")
                except Exception:
                    pass

    where_sql = " AND ".join(conditions)
    full_where = f"WHERE {where_sql}" if where_sql else ""

    dt_alias = "sales_data"
    base_cols = ", ".join(f"{dt_alias}.`{c}`" for c in SALES_COLUMNS)
    extra_cols = f"COALESCE(m2.Master_SKU, {dt_alias}.Sku) AS Master_SKU, COALESCE(im2.category, '') AS Category, COALESCE(im2.size, '') AS Size"
    join_sql = f"LEFT JOIN amazon_sku_code_mapping m2 ON {dt_alias}.Sku = m2.Master_SKU LEFT JOIN stock_items im2 ON {dt_alias}.Sku = im2.sku_code"

    inner_query = f"SELECT {base_cols}, {extra_cols} FROM {sales} {join_sql} {full_where}"

    dt_filter_conditions = []
    for key in qp.keys():
        if key.startswith("filter_"):
            col = key.replace("filter_", "", 1)
            if col in VALID_SORT_COLS:
                try:
                    vals = json.loads(qp.get(key))
                    if vals and isinstance(vals, list) and len(vals) > 0:
                        if vals == ["__NO_MATCH__"]:
                            dt_filter_conditions.append("1=0")
                        else:
                            escaped = [_sql_string(str(v)) for v in vals]
                            dt_filter_conditions.append(f"`{col}` IN ({', '.join(escaped)})")
                except Exception:
                    pass

    dt_where_sql = " AND ".join(dt_filter_conditions)
    dt_full_where = f"WHERE {dt_where_sql}" if dt_where_sql else ""

    count_sql = f"SELECT COUNT(*) as cnt FROM ({inner_query}) dt {dt_full_where}"
    total = int(_fetch_val(count_sql))
    total_pages = max(1, (total + page_size - 1) // page_size)

    order_sql = f"ORDER BY `{sort_by}` {sort_dir.upper()}"

    if export:
        offset = 0
        export_page_size = 5000
        def generate_csv():
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(SALES_COLUMNS + ["Master_SKU", "Category", "Size"])
            current_offset = 0
            while True:
                rows = _fetch(f"""
                    SELECT * FROM ({inner_query}) dt {dt_full_where}
                    {order_sql} LIMIT {export_page_size} OFFSET {current_offset}
                """)
                if not rows:
                    break
                for row in rows:
                    writer.writerow([row.get(c, "") for c in SALES_COLUMNS + ["Master_SKU", "Category", "Size"]])
                buffer.seek(0)
                yield buffer.read()
                buffer.seek(0)
                buffer.truncate(0)
                if len(rows) < export_page_size:
                    break
                current_offset += export_page_size

        return StreamingResponse(
            generate_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sales_data.csv"}
        )

    offset = (page - 1) * page_size
    rows = _fetch(f"""
        SELECT * FROM ({inner_query}) dt {dt_full_where}
        {order_sql} LIMIT {page_size} OFFSET {offset}
    """)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "rows": rows
    }


@router.get("/data-table-options")
async def get_data_table_options(
    request: Request,
    filters: dict[str, str] = Depends(get_sales_filters),
    user=Depends(get_current_user)
):
    """Get distinct values for a column to populate filter dropdowns."""
    source = filters["source"]
    sales = _sales_table(source, filters=filters)

    qp = request.query_params
    column = _clean_filter_value(qp.get("column"))
    transaction_type = _clean_filter_value(qp.get("transaction_type"))

    VALID_COLS = ["Seller_Gstin", "Invoice_Number", "Transaction_Type",
                  "Order_Id", "Sku", "Master_SKU", "Category", "Size", "Asin",
                  "Fulfillment_Channel", "Warehouse_Id",
                  "Ship_From_State", "Ship_To_State", "Ship_To_City"]
    if column not in VALID_COLS:
        return {"values": []}

    conditions = []
    if transaction_type and transaction_type.lower() != "all":
        tt = transaction_type
        if tt == "Refund":
            conditions.append("Transaction_Type IN ('Refund','EInvoiceCancel')")
        else:
            conditions.append(f"Transaction_Type = {_sql_string(tt)}")

    for key in qp.keys():
        if key.startswith("filter_"):
            col = key.replace("filter_", "", 1)
            if col in VALID_COLS:
                try:
                    vals = json.loads(qp.get(key))
                    if vals and isinstance(vals, list) and len(vals) > 0:
                        if vals == ["__NO_MATCH__"]:
                            conditions.append("1=0")
                        else:
                            escaped = [_sql_string(str(v)) for v in vals]
                            conditions.append(f"`{col}` IN ({', '.join(escaped)})")
                except Exception:
                    pass

    where_sql = " AND ".join(conditions)
    full_where = f"WHERE {where_sql}" if where_sql else ""

    dt_alias = "sales_data"

    if column in ("Master_SKU", "Category", "Size"):
        join_sql = f"LEFT JOIN amazon_sku_code_mapping m2 ON {dt_alias}.Sku = m2.Master_SKU LEFT JOIN stock_items im2 ON {dt_alias}.Sku = im2.sku_code"
        if column == "Master_SKU":
            select_col = f"COALESCE(m2.Master_SKU, {dt_alias}.Sku)"
        elif column == "Category":
            select_col = "im2.category"
        elif column == "Size":
            select_col = "im2.size"
        rows = _fetch(f"""
            SELECT DISTINCT {select_col} AS value FROM {sales} {join_sql} {full_where}
            AND {select_col} IS NOT NULL AND {select_col} != ''
            ORDER BY value LIMIT 500
        """)
    else:
        rows = _fetch(f"""
            SELECT DISTINCT {dt_alias}.`{column}` AS value FROM {sales} {full_where}
            AND {dt_alias}.`{column}` IS NOT NULL AND {dt_alias}.`{column}` != ''
            ORDER BY value LIMIT 500
        """)

    return {"values": [r["value"] for r in rows if r.get("value") not in (None, "")][:500]}
