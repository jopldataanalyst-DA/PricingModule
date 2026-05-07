import time
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Request, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import csv
import io
from pathlib import Path
from datetime import datetime
from auth import get_current_user, check_page_access
import polars as pl
import requests
from io import BytesIO
import time
from data_pipeline import run_pipeline

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



SELECTED_COLUMNS = [
    "sku_code", "item_name", "size", "category", "location",
    "cost", "price", "catalog", "mrp", "up_price",
    "available_atp", "fba_stock", "fbf_stock", "sjit_stock", "updated"
]

COLUMN_LABELS = {
    "sku_code": "Master SKU", "item_name": "Style ID / Parent SKU", "size": "Size",
    "category": "Category", "location": "Location", "cost": "Cost",
    "price": "Wholesale Price", "catalog": "Catalog Name", "mrp": "MRP", "up_price": "Up Price",
    "available_atp": "Uniware Stock", "fba_stock": "FBA", "fbf_stock": "FBF",
    "sjit_stock": "SJIT", "updated": "Launch Date"
}


@router.get("/columns")
async def get_columns(user=Depends(get_current_user)):
    if user.get('role') == 'admin':
        visible = SELECTED_COLUMNS
        editable = SELECTED_COLUMNS
    else:
        visible = user.get('visible_columns', SELECTED_COLUMNS[:5])
        editable = [c for c in visible if c not in ['sku_code']]
    
    return {"visible": visible, "editable": editable, "labels": COLUMN_LABELS}


@router.get("/filters")
async def get_filters(user=Depends(get_current_user)):
    items = load_items_db()
    categories = sorted(set(str(x.get('category', '')) for x in items if x.get('category')))
    return {"statuses": [], "categories": [c for c in categories if c]}


@router.get("/")
async def get_items(
    page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500),
    search: str = Query(""), status_filter: str = Query(""),
    availability: str = Query(""), category: str = Query(""),
    sort_by: str = Query("id"), sort_dir: str = Query("asc"),
    user=Depends(get_current_user)
):
    check_page_access(user, "item_master")
    
    items = load_items_db()
    all_items = [{**item, "id": i + 1} for i, item in enumerate(items)]
    
    # Calculate Global Stats (before filtering)
    stats = {
        "total_skus": len(all_items),
        "total_stock": sum(int(x.get('available_atp') or 0) for x in all_items),
        "total_available": sum(int(x.get('available_atp') or 0) for x in all_items),
        "total_fba": sum(int(x.get('fba_stock') or 0) for x in all_items),
        "total_sjit": sum(int(x.get('sjit_stock') or 0) for x in all_items),
        "total_fbf": sum(int(x.get('fbf_stock') or 0) for x in all_items)
    }
    
    item_list = all_items
    
    if search:
        q = search.lower()
        item_list = [x for x in item_list if q in str(x.get('sku_code', '')).lower() or q in str(x.get('item_name', '')).lower()]
    
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
    check_page_access(user, "edit_items")
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT sku_code, item_name, size, category, location, cost, price, catalog, mrp, available_atp, fba_stock, fbf_stock, sjit_stock, updated FROM stock_items")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Create header with labels
    header_labels = [COLUMN_LABELS.get(c, c) for c in [
        "sku_code", "item_name", "size", "category", "location",
        "cost", "price", "catalog", "mrp",
        "available_atp", "fba_stock", "fbf_stock", "sjit_stock",
        "updated"
    ]] + ["Action"]
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header_labels)
    
    for row in rows:
        row_data = [
            row.get("sku_code"), row.get("item_name"), row.get("size"), 
            row.get("category"), row.get("location"), row.get("cost"), 
            row.get("price"), row.get("catalog"), row.get("mrp"),
            row.get("available_atp"), row.get("fba_stock"), row.get("fbf_stock"), 
            row.get("sjit_stock"), row.get("updated"), ""
        ]
        writer.writerow(row_data)

    filename = "dashboard_export_" + datetime.now().strftime("%Y%m%d") + ".csv"
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
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
async def update_item(item_id: int, user=Depends(get_current_user)):
    check_page_access(user, "edit_items")
    from database import update_item_csv
    return await update_item_csv(item_id, user)


@router.delete("/{item_id}")
async def delete_item(item_id: int, user=Depends(get_current_user)):
    check_page_access(user, "edit_items")
    raise HTTPException(status_code=501, detail="Delete not implemented")


@router.post("/import")
async def import_items(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...), 
    user=Depends(get_current_user)
):
    """
    Import a dashboard CSV with human-readable labels and an 'Action' column.
    - Only rows with a non-empty Action are processed.
    - add    : insert into item_master + catalog_pricing
    - replace: update item_master + catalog_pricing
    - delete : delete from item_master + catalog_pricing
    """
    check_page_access(user, "edit_items")
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
                    INSERT INTO item_master (`Master SKU`, `Style ID / Parent SKU`, Size, Category, Loc)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sku, get_val("item_name"), get_val("size"), get_val("category"), get_val("location")))

                cursor.execute("""
                    INSERT INTO catalog_pricing (master_sku, launch_date, catalog_name, cost, wholesale_price, up_price)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        launch_date=VALUES(launch_date), catalog_name=VALUES(catalog_name),
                        cost=VALUES(cost), wholesale_price=VALUES(wholesale_price), up_price=VALUES(up_price)
                """, (sku, get_val("updated"), get_val("catalog"), f("cost"), f("price"), f("mrp")))
                added += 1

            elif action == "replace":
                cursor.execute("""
                    UPDATE item_master
                    SET `Style ID / Parent SKU`=%s, Size=%s, Category=%s, Loc=%s
                    WHERE `Master SKU`=%s
                """, (get_val("item_name"), get_val("size"), get_val("category"), get_val("location"), sku))

                cursor.execute("""
                    INSERT INTO catalog_pricing (master_sku, launch_date, catalog_name, cost, wholesale_price, up_price)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        launch_date=VALUES(launch_date), catalog_name=VALUES(catalog_name),
                        cost=VALUES(cost), wholesale_price=VALUES(wholesale_price), up_price=VALUES(up_price)
                """, (sku, get_val("updated"), get_val("catalog"), f("cost"), f("price"), f("mrp")))
                replaced += 1

            elif action == "delete":
                cursor.execute("DELETE FROM item_master WHERE `Master SKU`=%s", (sku,))
                cursor.execute("DELETE FROM catalog_pricing WHERE master_sku=%s", (sku,))
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

    # Trigger pipeline automatically to update dashboard
    background_tasks.add_task(run_pipeline)

    total = added + replaced + deleted
    return {
        "total": total,
        "added": added,
        "replaced": replaced,
        "deleted": deleted,
        "errors": errors,
        "error_details": error_details[:20]
    }