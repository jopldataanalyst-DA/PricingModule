from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any
from auth import get_current_user, check_page_access
from database import get_db
import json
import csv
import io
from datetime import datetime

router = APIRouter()

ALL_COLUMNS = [
    "source", "Invoice_Date", "Order_Id", "Sku", "Transaction_Type", "Quantity", "Invoice_Amount",
    "Ship_To_State", "Ship_To_City", "Fulfillment_Channel", "Warehouse_Id",
    "Item_Description", "Asin", "Hsn/sac", "Seller_Gstin",
    "Principal_Amount", "Shipping_Amount", "Total_Tax_Amount",
    "Tax_Exclusive_Gross", "Order_Date", "Shipment_Date", "Shipment_Id",
    "Ship_From_State", "Ship_From_City",
    "Bill_From_State", "Bill_From_City",
    "Buyer_Name", "Customer_Bill_To_Gstid",
    "Cgst_Rate", "Sgst_Rate", "Igst_Rate",
    "Cgst_Tax", "Sgst_Tax", "Igst_Tax",
    "Item_Promo_Discount", "Shipping_Promo_Discount",
    "Tcs_Igst_Amount", "Tcs_Cgst_Amount", "Tcs_Sgst_Amount",
    "Payment_Method_Code", "Credit_Note_No", "Credit_Note_Date",
    "Irn_Number", "Irn_Filing_Status", "Irn_Date",
]

DEFAULT_VISIBLE_COLUMNS = [
    "source", "Invoice_Date", "Order_Id", "Sku", "Transaction_Type", "Quantity", "Invoice_Amount",
    "Ship_To_State", "Fulfillment_Channel", "Item_Description",
]

COLUMN_LABELS = {
    "source": "Source", "Invoice_Date": "Invoice Date", "Order_Id": "Order ID",
    "Sku": "SKU", "Transaction_Type": "Type", "Quantity": "Qty",
    "Invoice_Amount": "Invoice Amount", "Ship_To_State": "Ship To State",
    "Ship_To_City": "Ship To City", "Fulfillment_Channel": "Fulfillment",
    "Warehouse_Id": "Warehouse", "Item_Description": "Item Description",
    "Asin": "ASIN", "Hsn/sac": "HSN/SAC", "Seller_Gstin": "Seller GSTIN",
    "Principal_Amount": "Principal", "Shipping_Amount": "Shipping",
    "Total_Tax_Amount": "Total Tax", "Tax_Exclusive_Gross": "Tax Excl Gross",
    "Order_Date": "Order Date", "Shipment_Date": "Shipment Date",
    "Shipment_Id": "Shipment ID", "Ship_From_State": "Ship From State",
    "Ship_From_City": "Ship From City", "Bill_From_State": "Bill From State",
    "Bill_From_City": "Bill From City",
    "Buyer_Name": "Buyer Name", "Customer_Bill_To_Gstid": "Buyer GSTIN",
    "Cgst_Rate": "CGST Rate", "Sgst_Rate": "SGST Rate", "Igst_Rate": "IGST Rate",
    "Cgst_Tax": "CGST Tax", "Sgst_Tax": "SGST Tax", "Igst_Tax": "IGST Tax",
    "Item_Promo_Discount": "Item Promo Discount",
    "Shipping_Promo_Discount": "Shipping Promo Discount",
    "Tcs_Igst_Amount": "TCS IGST", "Tcs_Cgst_Amount": "TCS CGST",
    "Tcs_Sgst_Amount": "TCS SGST", "Payment_Method_Code": "Payment Method",
    "Credit_Note_No": "Credit Note No", "Credit_Note_Date": "Credit Note Date",
    "Irn_Number": "IRN Number", "Irn_Filing_Status": "IRN Status",
    "Irn_Date": "IRN Date",
}

NUMERIC_COLUMNS = set([
    "Quantity", "Invoice_Amount", "Principal_Amount", "Shipping_Amount",
    "Total_Tax_Amount", "Tax_Exclusive_Gross",
    "Cgst_Rate", "Sgst_Rate", "Igst_Rate",
    "Cgst_Tax", "Sgst_Tax", "Igst_Tax",
    "Item_Promo_Discount", "Shipping_Promo_Discount",
    "Tcs_Igst_Amount", "Tcs_Cgst_Amount", "Tcs_Sgst_Amount",
])

SEARCHABLE_COLUMNS = ["Sku", "Order_Id", "Item_Description", "Ship_To_State", "Asin", "Ship_To_City", "Buyer_Name", "Invoice_Number"]

# Columns shared by both tables (after aliasing)
UNION_COLUMNS = [
    "Seller_Gstin", "Invoice_Number", "Invoice_Date",
    "Transaction_Type", "Order_Id", "Shipment_Id",
    "Shipment_Date", "Order_Date", "Shipment_Item_Id",
    "Quantity", "Item_Description", "Asin",
    "Hsn/sac", "Sku", "Product_Tax_Code",
    "Bill_From_City", "Bill_From_State", "Bill_From_Country", "Bill_From_Postal_Code",
    "Ship_From_City", "Ship_From_State", "Ship_From_Country", "Ship_From_Postal_Code",
    "Ship_To_City", "Ship_To_State", "Ship_To_Country", "Ship_To_Postal_Code",
    "Invoice_Amount", "Tax_Exclusive_Gross", "Total_Tax_Amount",
    "Cgst_Rate", "Sgst_Rate", "Utgst_Rate", "Igst_Rate",
    "Compensatory_Cess_Rate", "Principal_Amount", "Principal_Amount_Basis",
    "Cgst_Tax", "Sgst_Tax", "Igst_Tax", "Utgst_Tax", "Compensatory_Cess_Tax",
    "Shipping_Amount", "Shipping_Amount_Basis",
    "Shipping_Cgst_Tax", "Shipping_Sgst_Tax", "Shipping_Utgst_Tax", "Shipping_Igst_Tax",
    "Gift_Wrap_Amount", "Gift_Wrap_Amount_Basis",
    "Gift_Wrap_Cgst_Tax", "Gift_Wrap_Sgst_Tax", "Gift_Wrap_Utgst_Tax", "Gift_Wrap_Igst_Tax",
    "Gift_Wrap_Compensatory_Cess_Tax",
    "Item_Promo_Discount", "Item_Promo_Discount_Basis", "Item_Promo_Tax",
    "Shipping_Promo_Discount", "Shipping_Promo_Discount_Basis", "Shipping_Promo_Tax",
    "Gift_Wrap_Promo_Discount", "Gift_Wrap_Promo_Discount_Basis", "Gift_Wrap_Promo_Tax",
    "Tcs_Cgst_Rate", "Tcs_Cgst_Amount",
    "Tcs_Sgst_Rate", "Tcs_Sgst_Amount",
    "Tcs_Utgst_Rate", "Tcs_Utgst_Amount",
    "Tcs_Igst_Rate", "Tcs_Igst_Amount",
    "Warehouse_Id", "Fulfillment_Channel", "Payment_Method_Code",
    "Credit_Note_No", "Credit_Note_Date",
    # B2B-only cols (NULL for B2C)
    "Bill_To_City", "Bill_To_State", "Bill_To_Country", "Bill_To_Postalcode",
    "Customer_Bill_To_Gstid", "Customer_Ship_To_Gstid", "Buyer_Name",
    "Irn_Number", "Irn_Filing_Status", "Irn_Date", "Irn_Error_Code",
]

UNION_SELECT_B2B = "SELECT 'B2B' AS source, " + ", ".join(f"`{c}`" for c in UNION_COLUMNS) + " FROM amazon_sales_b2b"

B2C_UNION_COLUMNS = [
    # Same as UNION_COLUMNS but with Shipping_Cess_Tax_Amount → Shipping_Cess_Tax mapping
    # and NULL for B2B-only columns (those are the last 11)
]
B2C_COMMON = UNION_COLUMNS[:-11]  # All common + Shipping_Cess_Tax
B2C_MAPPED = []
for c in B2C_COMMON:
    if c == "Shipping_Cess_Tax":
        B2C_MAPPED.append("`Shipping_Cess_Tax_Amount` AS `Shipping_Cess_Tax`")
    else:
        B2C_MAPPED.append(f"`{c}`")
B2C_NULL = ["NULL AS `" + c + "`" for c in UNION_COLUMNS[-11:]]
UNION_SELECT_B2C = "SELECT 'B2C' AS source, " + ", ".join(B2C_MAPPED + B2C_NULL) + " FROM amazon_sales_b2c"

UNION_BODY = f"({UNION_SELECT_B2B}) UNION ALL ({UNION_SELECT_B2C})"


def q(col):
    return f"`{col}`"


def build_search_clause(search: str) -> str:
    if not search:
        return ""
    s = search.lower().replace("'", "''")
    clauses = " OR ".join(f"LOWER({q(col)}) LIKE '%{s}%'" for col in SEARCHABLE_COLUMNS)
    return f"AND ({clauses})"


def build_filter_clauses(filters: dict):
    if not filters:
        return "", []
    where_parts = []
    params = []
    for col, values in filters.items():
        if col not in ALL_COLUMNS or not isinstance(values, list) or not values:
            continue
        placeholders = ", ".join("%s" for _ in values)
        where_parts.append(f"{q(col)} IN ({placeholders})")
        params.extend(values)
    return ("AND " + " AND ".join(where_parts)) if where_parts else "", params


@router.get("/columns")
async def get_columns(user=Depends(get_current_user)):
    check_page_access(user, "amazon_pricing")
    return {"visible": DEFAULT_VISIBLE_COLUMNS, "all": ALL_COLUMNS, "editable": [], "labels": COLUMN_LABELS}


@router.get("/filter-options")
async def get_filter_options(
    column: str = Query(""),
    search: str = Query(""),
    filters: str = Query(""),
    user=Depends(get_current_user)
):
    check_page_access(user, "amazon_pricing")
    if column not in ALL_COLUMNS:
        return {"column": column, "values": []}

    active_filters = json.loads(filters) if filters else {}
    filter_clause, filter_params = build_filter_clauses(active_filters)
    search_clause = ""
    if search:
        search_clause = build_search_clause(search)

    col = q(column)
    sql = f"""
        SELECT DISTINCT {col} AS val FROM {UNION_BODY} combined
        WHERE {col} IS NOT NULL AND {col} != ''
        ORDER BY val
    """

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(sql)
        values = [str(r[0]) for r in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Filter options error: {e}")
        values = []

    return {"column": column, "values": values}


@router.get("/")
async def get_amazon_pricing(
    page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500),
    search: str = Query(""), sort_by: str = Query("Invoice_Date"), sort_dir: str = Query("desc"),
    filters: str = Query(""),
    user=Depends(get_current_user)
):
    check_page_access(user, "amazon_pricing")

    active_filters = json.loads(filters) if filters else {}
    filter_clause, filter_params = build_filter_clauses(active_filters)
    search_clause = ""
    if search:
        search_clause = build_search_clause(search)

    base_from = f"FROM ({UNION_SELECT_B2B} UNION ALL {UNION_SELECT_B2C}) combined WHERE 1=1 {search_clause} {filter_clause}"

    try:
        conn = get_db()
        cursor = conn.cursor()

        count_sql = f"SELECT COUNT(*) {base_from}"
        cursor.execute(count_sql, filter_params)
        total = cursor.fetchone()[0]

        stats_sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                COALESCE(SUM(Invoice_Amount), 0) AS total_invoice_amount,
                COALESCE(SUM(Quantity), 0) AS total_quantity
            {base_from}
        """
        cursor.execute(stats_sql, filter_params)
        sr = cursor.fetchone()
        stats = {
            "total_rows": sr[0],
            "total_invoice_amount": float(sr[1]) if sr[1] else 0,
            "total_quantity": float(sr[2]) if sr[2] else 0,
            "b2b_rows": 0,
            "b2c_rows": 0,
        }

        safe_name = q(sort_by) if sort_by in ALL_COLUMNS else "Invoice_Date"
        order = "DESC" if sort_dir == "desc" else "ASC"
        offset = (page - 1) * page_size

        data_sql = f"""
            SELECT * {base_from}
            ORDER BY {safe_name} {order}
            LIMIT %s OFFSET %s
        """
        cursor.execute(data_sql, filter_params + [page_size, offset])
        col_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(col_names, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        for i, row in enumerate(rows):
            row["id"] = offset + i + 1
            src = row.get("source")
            if src == "B2B":
                stats["b2b_rows"] = stats.get("b2b_rows", 0) + 1
            else:
                stats["b2c_rows"] = stats.get("b2c_rows", 0) + 1

        total_pages = max(1, (total + page_size - 1) // page_size)
        return {"items": rows, "total": total, "page": page, "page_size": page_size,
                "total_pages": total_pages, "stats": stats}

    except Exception as e:
        print(f"Error querying combined sales: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Query error: {e}")


@router.get("/export")
async def export_amazon_pricing(user=Depends(get_current_user)):
    check_page_access(user, "amazon_pricing")

    try:
        conn = get_db()
        cursor = conn.cursor()
        sql = f"{UNION_SELECT_B2B} UNION ALL {UNION_SELECT_B2C}"
        cursor.execute(sql)
        col_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(col_names, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=f"Export error: {e}")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([COLUMN_LABELS.get(col, col) for col in ALL_COLUMNS])
    for row in rows:
        writer.writerow([row.get(col, "") for col in ALL_COLUMNS])

    filename = "amazon_sales_export_" + datetime.now().strftime("%Y%m%d") + ".csv"
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
