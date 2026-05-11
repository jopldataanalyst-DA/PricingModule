import time
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Request, Query, BackgroundTasks, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import csv
import io
import json
from pathlib import Path
from datetime import datetime
from auth import get_current_user, check_page_access, verify_special_password
import polars as pl
import requests
from io import BytesIO
import time
from data_pipeline import run_pipeline
from audit import record_audit_log, record_import_history

router = APIRouter()

DATA_DIR = Path(__file__).parent.parent.parent / "Data"
ITEM_MASTER_CSV = DATA_DIR / "ItemMaster.csv"
ITEM_MASTER_SELECTED_CSV = DATA_DIR / "ItemMaster_selected.csv"
STOCK_UPDATE_CSV = DATA_DIR / "StockUpdate_selected.csv"
STOCK_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"
from database import get_db

def load_items_db() -> List[Dict[str, Any]]:
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM stock_items")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return list(rows)
    except Exception as e:
        print(f"Error loading items from DB: {e}")
        return []


def count_item_master_rows() -> int:
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM item_master")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return int(count or 0)
    except Exception as e:
        print(f"Error counting item_master rows: {e}")
        return 0



SELECTED_COLUMNS = [
    "sku_code", "item_name", "size", "category", "location",
    "child_remark", "parent_remark", "item_type",
    "cost", "price", "catalog", "mrp", "up_price", "cost_into_percent",
    "available_atp", "fba_stock", "fbf_stock", "sjit_stock", "updated"
]

COLUMN_LABELS = {
    "sku_code": "Master SKU", "item_name": "Style ID / Parent SKU", "size": "Size",
    "category": "Category", "location": "Location", "cost": "Cost",
    "child_remark": "Child Remark", "parent_remark": "Parent Remark",
    "item_type": "Type",
    "price": "Wholesale Price", "catalog": "Catalog Name", "mrp": "MRP", "up_price": "Up Price",
    "cost_into_percent": "Cost into %",
    "available_atp": "Uniware Stock", "fba_stock": "FBA", "fbf_stock": "FBF",
    "sjit_stock": "SJIT", "updated": "Launch Date"
}

ITEM_MASTER_DB_COLUMNS = {
    "sku_code": "`Master SKU`",
    "item_name": "`Style ID / Parent SKU`",
    "size": "Size",
    "category": "Category",
    "location": "Loc",
    "child_remark": "`Child Remark`",
    "parent_remark": "`Parent Remark`",
    "item_type": "`Type`",
}

CATALOG_PRICING_DB_COLUMNS = {
    "updated": "launch_date",
    "catalog": "catalog_name",
    "cost": "cost",
    "price": "wholesale_price",
    "up_price": "up_price",
    "mrp": "mrp",
}


CATALOG_PRICING_COLUMN_ALIASES = {
    "master_sku": ["master_sku", "Master SKU"],
    "launch_date": ["launch_date", "Launch Date"],
    "catalog_name": ["catalog_name", "Catalog Name"],
    "cost": ["cost", "Cost"],
    "wholesale_price": ["wholesale_price", "Wholesale Price"],
    "up_price": ["up_price", "Up Price"],
    "mrp": ["mrp", "MRP"],
}


def quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def get_catalog_pricing_columns(cursor) -> dict[str, str]:
    cursor.execute("SHOW COLUMNS FROM catalog_pricing")
    existing = {row[0] for row in cursor.fetchall()}
    columns = {}
    for key, aliases in CATALOG_PRICING_COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in existing:
                columns[key] = quote_identifier(alias)
                break
    if "master_sku" not in columns:
        raise HTTPException(status_code=500, detail="catalog_pricing is missing a Master SKU column")
    return columns


def upsert_catalog_pricing(cursor, sku: str, values: dict[str, Any]):
    columns = get_catalog_pricing_columns(cursor)
    sku_col = columns["master_sku"]
    cursor.execute(f"SELECT COUNT(*) FROM catalog_pricing WHERE {sku_col}=%s", (sku,))
    exists = cursor.fetchone()[0] > 0

    values = {key: value for key, value in values.items() if key in columns}
    if exists:
        if values:
            cursor.execute(
                "UPDATE catalog_pricing SET "
                + ", ".join(f"{columns[key]}=%s" for key in values)
                + f" WHERE {sku_col}=%s",
                (*values.values(), sku),
            )
    else:
        insert_columns = ["master_sku", *values.keys()]
        cursor.execute(
            "INSERT INTO catalog_pricing ("
            + ", ".join(columns[key] for key in insert_columns)
            + ") VALUES ("
            + ", ".join(["%s"] * len(insert_columns))
            + ")",
            (sku, *values.values()),
        )


def delete_catalog_pricing(cursor, sku: str):
    columns = get_catalog_pricing_columns(cursor)
    cursor.execute(f"DELETE FROM catalog_pricing WHERE {columns['master_sku']}=%s", (sku,))


class ItemChangeRequest(BaseModel):
    change_remark: str
    special_password: Optional[str] = None
    sku_code: Optional[str] = None
    item_name: Optional[str] = None
    size: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None
    child_remark: Optional[str] = None
    parent_remark: Optional[str] = None
    item_type: Optional[str] = None
    cost: Optional[float] = None
    price: Optional[float] = None
    catalog: Optional[str] = None
    mrp: Optional[float] = None
    up_price: Optional[float] = None
    cost_into_percent: Optional[float] = None
    available_atp: Optional[int] = None
    fba_stock: Optional[int] = None
    fbf_stock: Optional[int] = None
    sjit_stock: Optional[int] = None
    updated: Optional[str] = None


def editable_columns_for_user(user: dict) -> list[str]:
    if user.get("role") == "admin":
        return list(SELECTED_COLUMNS)
    permissions = user.get("column_permissions", {}).get("item_master", {})
    visible = permissions.get("visible") or []
    editable = permissions.get("editable") or []
    return [c for c in editable if c in visible and c in SELECTED_COLUMNS]


def require_change_remark(change_remark: str):
    if not change_remark or not change_remark.strip():
        raise HTTPException(status_code=400, detail="Change remark is required")
    return change_remark.strip()


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
            if q in str(x.get('sku_code', '')).lower()
            or q in str(x.get('item_name', '')).lower()
        ]

    filters = filters or {}
    for col, values in filters.items():
        if col == skip_column or col not in SELECTED_COLUMNS:
            continue
        if not isinstance(values, list) or not values:
            continue
        allowed = {display_value(v) for v in values}
        filtered = [x for x in filtered if display_value(x.get(col)) in allowed]

    return filtered


@router.get("/columns")
async def get_columns(user=Depends(get_current_user)):
    if user.get('role') == 'admin':
        visible = SELECTED_COLUMNS
        editable = SELECTED_COLUMNS
        all_columns = SELECTED_COLUMNS
    else:
        permissions = user.get("column_permissions", {}).get("item_master", {})
        visible = permissions.get("visible") or SELECTED_COLUMNS[:5]
        editable = permissions.get("editable") or []
        visible = [c for c in visible if c in SELECTED_COLUMNS]
        editable = [c for c in editable if c in visible and c in SELECTED_COLUMNS]
        all_columns = visible
    
    return {"visible": visible, "all": all_columns, "editable": editable, "labels": COLUMN_LABELS}


@router.get("/filters")
async def get_filters(user=Depends(get_current_user)):
    items = load_items_db()
    categories = sorted(set(str(x.get('category', '')) for x in items if x.get('category')))
    return {"statuses": [], "categories": [c for c in categories if c]}


@router.get("/filter-options")
async def get_filter_options(
    column: str = Query(""),
    search: str = Query(""),
    filters: str = Query(""),
    user=Depends(get_current_user)
):
    check_page_access(user, "item_master")
    if column not in SELECTED_COLUMNS:
        return {"column": column, "values": []}

    items = [{**item, "id": i + 1} for i, item in enumerate(load_items_db())]
    active_filters = parse_json_dict(filters)
    items = apply_search_and_filters(items, search=search, filters=active_filters, skip_column=column)
    values = sorted({display_value(row.get(column)) for row in items}, key=lambda value: value.lower())
    return {"column": column, "values": values}


@router.get("/")
async def get_items(
    page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500),
    search: str = Query(""), status_filter: str = Query(""),
    availability: str = Query(""), category: str = Query(""),
    sort_by: str = Query("id"), sort_dir: str = Query("asc"),
    filters: str = Query(""),
    user=Depends(get_current_user)
):
    check_page_access(user, "item_master")
    
    items = load_items_db()
    all_items = [{**item, "id": i + 1} for i, item in enumerate(items)]
    
    # Calculate Global Stats (before filtering)
    total_skus = count_item_master_rows() or len(all_items)
    total_available = sum(int(x.get('available_atp') or 0) for x in all_items)
    total_fba = sum(int(x.get('fba_stock') or 0) for x in all_items)
    total_sjit = sum(int(x.get('sjit_stock') or 0) for x in all_items)
    total_fbf = sum(int(x.get('fbf_stock') or 0) for x in all_items)
    stats = {
        "total_skus": total_skus,
        "total_stock": total_available + total_fba + total_sjit + total_fbf,
        "total_available": total_available,
        "total_fba": total_fba,
        "total_sjit": total_sjit,
        "total_fbf": total_fbf
    }
    
    active_filters = parse_json_dict(filters)
    item_list = apply_search_and_filters(all_items, search=search, filters=active_filters)
    
    if status_filter:
        item_list = [x for x in item_list if str(x.get('status', '')).lower() == status_filter.lower()]
    
    if availability:
        avail_val = availability.lower()
        item_list = [x for x in item_list if str(x.get('available_atp', 0)).lower() == avail_val or (avail_val == 'yes' and int(x.get('available_atp', 0)) > 0) or (avail_val == 'no' and int(x.get('available_atp', 0)) <= 0)]
    
    if category:
        item_list = [x for x in item_list if category.lower() in str(x.get('category', '')).lower()]
    
    if sort_by and sort_dir:
        reverse = sort_dir == 'desc'
        numeric_cols = ['id', 'available_atp', 'fba_stock', 'sjit_stock', 'fbf_stock', 'cost', 'price', 'mrp']
        if sort_by in numeric_cols:
            item_list.sort(key=lambda x: float(x.get(sort_by, 0) or 0), reverse=reverse)
        else:
            item_list.sort(key=lambda x: (str(x.get(sort_by, '')) or '').lower(), reverse=reverse)
    
    total = len(item_list)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = item_list[start:end]
    
    return {"items": page_items, "total": total, "page": page, "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total else 1, "stats": stats}


@router.get("/export")
async def export_items(user=Depends(get_current_user)):
    """Export current dashboard data as CSV with a blank Action column."""
    check_page_access(user, "item_master")
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT sku_code, item_name, size, category, location, child_remark, parent_remark, item_type, cost, price, catalog, mrp, up_price, cost_into_percent, available_atp, fba_stock, fbf_stock, sjit_stock, updated FROM stock_items")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Create header with labels
    header_labels = [COLUMN_LABELS.get(c, c) for c in [
        "sku_code", "item_name", "size", "category", "location",
        "child_remark", "parent_remark", "item_type",
        "cost", "price", "catalog", "mrp", "up_price", "cost_into_percent",
        "available_atp", "fba_stock", "fbf_stock", "sjit_stock", "updated"
    ]] + ["Action"]
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header_labels)
    
    for row in rows:
        row_data = [
            row.get("sku_code"), row.get("item_name"), row.get("size"), 
            row.get("category"), row.get("location"), row.get("child_remark"),
            row.get("parent_remark"), row.get("item_type"), row.get("cost"), 
            row.get("price"), row.get("catalog"), row.get("mrp"), row.get("up_price"),
            row.get("cost_into_percent"),
            row.get("available_atp"), row.get("fba_stock"), row.get("fbf_stock"), 
            row.get("sjit_stock"), row.get("updated"), ""
        ]
        writer.writerow(row_data)

    filename = "dashboard_export_" + datetime.now().strftime("%Y%m%d") + ".csv"
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get("/{item_id}")
async def get_item(item_id: int, user=Depends(get_current_user)):
    check_page_access(user, "item_master")
    item_list = load_items_db()
    if item_id < 1 or item_id > len(item_list):
        raise HTTPException(status_code=404, detail="Item not found")
    return {"id": item_id, **item_list[item_id - 1]}


@router.put("/{item_id}")
async def update_item(item_id: int, payload: ItemChangeRequest, user=Depends(get_current_user)):
    check_page_access(user, "item_master")
    change_remark = require_change_remark(payload.change_remark)
    verify_special_password(user, payload.special_password)

    item_list = load_items_db()
    if item_id < 1 or item_id > len(item_list):
        raise HTTPException(status_code=404, detail="Item not found")

    current = item_list[item_id - 1]
    sku = current.get("sku_code")
    if not sku:
        raise HTTPException(status_code=400, detail="Item has no SKU")

    updates = payload.model_dump(exclude_unset=True) if hasattr(payload, "model_dump") else payload.dict(exclude_unset=True)
    updates.pop("change_remark", None)
    updates.pop("special_password", None)
    updates.pop("sku_code", None)
    updates = {k: v for k, v in updates.items() if k in SELECTED_COLUMNS}
    if not updates:
        raise HTTPException(status_code=400, detail="No item fields were changed")

    editable = editable_columns_for_user(user)
    blocked = [k for k in updates if k not in editable]
    if blocked:
        raise HTTPException(status_code=403, detail=f"No edit permission for: {', '.join(blocked)}")

    conn = get_db()
    cursor = conn.cursor()
    try:
        stock_sets = []
        stock_values = []
        for col, value in updates.items():
            stock_sets.append(f"{col}=%s")
            stock_values.append(value)
        if stock_sets:
            cursor.execute(
                f"UPDATE stock_items SET {', '.join(stock_sets)} WHERE sku_code=%s",
                (*stock_values, sku),
            )

        item_master_updates = [(ITEM_MASTER_DB_COLUMNS[col], value) for col, value in updates.items() if col in ITEM_MASTER_DB_COLUMNS]
        if item_master_updates:
            cursor.execute(
                "UPDATE item_master SET "
                + ", ".join(f"{col}=%s" for col, _ in item_master_updates)
                + " WHERE `Master SKU`=%s",
                (*[value for _, value in item_master_updates], sku),
            )

        catalog_updates = [(CATALOG_PRICING_DB_COLUMNS[col], value) for col, value in updates.items() if col in CATALOG_PRICING_DB_COLUMNS]
        if catalog_updates:
            upsert_catalog_pricing(cursor, sku, {col: value for col, value in catalog_updates})

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to update item: {e}")
    finally:
        cursor.close()
        conn.close()

    record_audit_log(
        user,
        "UPDATE_ITEM",
        table_name="stock_items",
        record_id=sku,
        remark=f"{change_remark} | Changed fields: {', '.join(sorted(updates.keys()))}",
    )
    return {"message": "Item updated", "sku_code": sku, "updated_fields": sorted(updates.keys())}


@router.delete("/{item_id}")
async def delete_item(item_id: int, user=Depends(get_current_user)):
    check_page_access(user, "edit_items")
    raise HTTPException(status_code=501, detail="Delete not implemented")


@router.post("/import")
async def import_items(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...), 
    change_remark: str = Form(...),
    special_password: Optional[str] = Form(None),
    user=Depends(get_current_user)
):
    """
    Import a dashboard CSV with human-readable labels and an 'Action' column.
    - Only rows with a non-empty Action are processed.
    - add    : insert into item_master + catalog_pricing
    - replace: update item_master + catalog_pricing
    - delete : delete from item_master + catalog_pricing
    """
    check_page_access(user, "import")
    change_remark = require_change_remark(change_remark)
    verify_special_password(user, special_password)
    content = await file.read()

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    fieldnames = reader.fieldnames or []

    if not rows:
        raise HTTPException(status_code=400, detail="CSV file is empty.")

    added = replaced = deleted = errors = 0
    error_details = []

    conn = get_db()
    cursor = conn.cursor()

    for i, row in enumerate(rows, 1):
        action = str(row.get("Action", "")).strip().lower()
        if not action:
            continue

        # Map row values using the labels
        def get_val(internal_key):
            label = COLUMN_LABELS.get(internal_key)
            return str(row.get(label, "") or "").strip()

        sku = get_val("sku_code")
        if not sku:
            errors += 1
            error_details.append(f"Row {i}: missing Master SKU, skipped.")
            continue

        def f(internal_key):
            val = get_val(internal_key)
            try: return float(val or 0)
            except: return 0.0

        try:
            if action == "add":
                cursor.execute("""
                    INSERT INTO item_master (`Master SKU`, `Style ID / Parent SKU`, Size, Category, Loc, `Child Remark`, `Parent Remark`, `Type`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    sku, get_val("item_name"), get_val("size"), get_val("category"),
                    get_val("location"), get_val("child_remark"),
                    get_val("parent_remark"), get_val("item_type")
                ))

                upsert_catalog_pricing(cursor, sku, {
                    "launch_date": get_val("updated"),
                    "catalog_name": get_val("catalog"),
                    "cost": f("cost"),
                    "wholesale_price": f("price"),
                    "up_price": f("up_price"),
                    "mrp": f("mrp"),
                })
                added += 1

            elif action == "replace":
                cursor.execute("""
                    UPDATE item_master
                    SET `Style ID / Parent SKU`=%s, Size=%s, Category=%s, Loc=%s,
                        `Child Remark`=%s, `Parent Remark`=%s, `Type`=%s
                    WHERE `Master SKU`=%s
                """, (
                    get_val("item_name"), get_val("size"), get_val("category"),
                    get_val("location"), get_val("child_remark"),
                    get_val("parent_remark"), get_val("item_type"), sku
                ))

                upsert_catalog_pricing(cursor, sku, {
                    "launch_date": get_val("updated"),
                    "catalog_name": get_val("catalog"),
                    "cost": f("cost"),
                    "wholesale_price": f("price"),
                    "up_price": f("up_price"),
                    "mrp": f("mrp"),
                })
                replaced += 1

            elif action == "delete":
                cursor.execute("DELETE FROM item_master WHERE `Master SKU`=%s", (sku,))
                delete_catalog_pricing(cursor, sku)
                deleted += 1

            else:
                errors += 1
                error_details.append(f"Row {i} (SKU: {sku}): unknown action '{action}'.")

        except Exception as e:
            errors += 1
            error_details.append(f"Row {i} (SKU: {sku}): {str(e)}")

    conn.commit()
    cursor.close()
    conn.close()

    total = added + replaced + deleted

    # Trigger pipeline automatically to update dashboard
    background_tasks.add_task(run_pipeline)
    status = "success" if errors == 0 else ("partial" if total > 0 else "failed")
    record_import_history(
        filename=file.filename or "uploaded.csv",
        imported_by=user.get("username", "system"),
        total_rows=len(rows),
        new_rows=added,
        updated_rows=replaced,
        deleted_rows=deleted,
        skipped_rows=max(len(rows) - total - errors, 0),
        error_rows=errors,
        status=status,
        details=error_details[:20],
    )
    record_audit_log(
        user,
        "IMPORT_ITEMS",
        table_name="stock_items",
        remark=f"{change_remark} | File: {file.filename}; added={added}; replaced={replaced}; deleted={deleted}; errors={errors}",
    )

    return {
        "total": total,
        "added": added,
        "replaced": replaced,
        "deleted": deleted,
        "errors": errors,
        "error_details": error_details[:20]
    }
