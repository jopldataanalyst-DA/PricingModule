"""Database, user, and permission bootstrap helpers.

Use case:
    Centralizes MySQL connection settings, creates all tables needed by the
    application, manages the JSON-backed user store, and derives schemas for
    imported Amazon sales history CSVs.
"""

import json
from pathlib import Path
import hashlib
import csv
import sys
import os
from typing import Any

DATA_DIR = Path(__file__).parent.parent.parent / "Data"
AMAZON_SALES_HISTORY_DIR = DATA_DIR / "AmazonData" / "AmazonSalesHistory"
AMAZON_SALES_TABLES = {
    "b2b": "amazon_sales_b2b",
    "b2c": "amazon_sales_b2c",
}

def hash_password(password: str) -> str:
    """Return the SHA-256 hash used for local user passwords."""
    return hashlib.sha256(password.encode()).hexdigest()

USERS_FILE = DATA_DIR / "users.json"

AMAZON_PRICING_COLUMNS = [
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

AMAZON_PRICING_DEFAULT_VISIBLE = [
    "source", "Invoice_Date", "Order_Id", "Sku", "Transaction_Type", "Quantity", "Invoice_Amount",
    "Ship_To_State", "Fulfillment_Channel", "Item_Description",
]

DEFAULT_COLUMN_PERMISSIONS = {
    "item_master": {
        "visible": [
            "sku_code", "item_name", "size", "category", "location",
            "child_remark", "parent_remark", "item_type",
            "cost", "price", "catalog", "mrp", "up_price",
            "available_atp", "fba_stock", "fbf_stock", "sjit_stock", "updated"
        ],
        "editable": []
    },
    "amazon_pricing": {
        "visible": AMAZON_PRICING_DEFAULT_VISIBLE,
        "editable": []
    }
}

ADMIN_COLUMN_PERMISSIONS = {
    "item_master": {
        "visible": DEFAULT_COLUMN_PERMISSIONS["item_master"]["visible"],
        "editable": DEFAULT_COLUMN_PERMISSIONS["item_master"]["visible"]
    },
    "amazon_pricing": {
        "visible": AMAZON_PRICING_COLUMNS,
        "editable": []
    }
}

RESTRICTED_COLUMN_PERMISSIONS = {
    "item_master": {
        "visible": ["sku_code", "item_name", "size", "category", "available_atp"],
        "editable": []
    },
    "amazon_pricing": {
        "visible": ["source", "Invoice_Date", "Order_Id", "Sku", "Transaction_Type", "Quantity", "Invoice_Amount", "Ship_To_State", "Fulfillment_Channel"],
        "editable": []
    }
}

DEFAULT_USERS = [
    {"id": 1, "username": "admin", "password": hash_password("admin123"), "special_password": "", "role": "admin", "allowed_pages": ["item_master", "amazon_pricing", "admin", "logs", "import", "sales"], "column_permissions": ADMIN_COLUMN_PERMISSIONS, "is_active": True},
    {"id": 2, "username": "vikesh", "password": hash_password("vikesh123"), "special_password": hash_password("vikesh123"), "role": "viewer", "allowed_pages": ["item_master"], "column_permissions": DEFAULT_COLUMN_PERMISSIONS, "is_active": True},
    {"id": 3, "username": "hitesh", "password": hash_password("hitesh123"), "special_password": hash_password("hitesh123"), "role": "restricted", "allowed_pages": ["item_master"], "column_permissions": RESTRICTED_COLUMN_PERMISSIONS, "is_active": True},
]


def _as_list(value: Any, fallback: list[str]) -> list[str]:
    """Normalize persisted permission values into a list of strings."""
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except json.JSONDecodeError:
            return [value]
    return list(fallback)


def normalize_user(user: dict[str, Any], next_id: int | None = None) -> dict[str, Any]:
    """Apply role defaults and permission shape expected by the UI/API."""
    role = str(user.get("role") or "viewer").lower()
    if role == "admin":
        default_perms = ADMIN_COLUMN_PERMISSIONS
    elif role == "restricted":
        default_perms = RESTRICTED_COLUMN_PERMISSIONS
    else:
        default_perms = DEFAULT_COLUMN_PERMISSIONS
    allowed_default = ["item_master", "amazon_pricing", "admin", "logs", "import", "sales"] if role == "admin" else ["item_master"]
    normalized = dict(user)
    if not normalized.get("id") and next_id is not None:
        normalized["id"] = next_id
    normalized["username"] = str(normalized.get("username") or "").strip()
    normalized["role"] = role
    normalized["allowed_pages"] = _as_list(normalized.get("allowed_pages"), allowed_default)
    normalized["is_active"] = bool(normalized.get("is_active", True))
    if "special_password" not in normalized:
        normalized["special_password"] = ""

    column_permissions = normalized.get("column_permissions")
    if not isinstance(column_permissions, dict):
        column_permissions = default_perms
    item_master = column_permissions.get("item_master") if isinstance(column_permissions, dict) else None
    if not isinstance(item_master, dict):
        item_master = default_perms["item_master"]
    visible = _as_list(item_master.get("visible"), default_perms["item_master"]["visible"])
    editable = _as_list(item_master.get("editable"), default_perms["item_master"]["editable"])
    item_columns = set(DEFAULT_COLUMN_PERMISSIONS["item_master"]["visible"])
    visible = [col for col in visible if col in item_columns]
    editable = [col for col in editable if col in item_columns]
    normalized["column_permissions"] = {"item_master": {"visible": visible, "editable": editable}}

    amazon_pricing = column_permissions.get("amazon_pricing") if isinstance(column_permissions, dict) else None
    if not isinstance(amazon_pricing, dict):
        amazon_pricing = default_perms.get("amazon_pricing", {"visible": AMAZON_PRICING_DEFAULT_VISIBLE, "editable": []})
    ap_visible = _as_list(amazon_pricing.get("visible"), default_perms.get("amazon_pricing", {"visible": AMAZON_PRICING_DEFAULT_VISIBLE, "editable": []})["visible"])
    ap_editable = _as_list(amazon_pricing.get("editable"), default_perms.get("amazon_pricing", {"visible": AMAZON_PRICING_DEFAULT_VISIBLE, "editable": []})["editable"])
    normalized["column_permissions"]["amazon_pricing"] = {"visible": ap_visible, "editable": ap_editable}
    return normalized

def load_users():
    """Load users from Data/users.json, creating default users if missing."""
    if not USERS_FILE.exists():
        save_users(DEFAULT_USERS)
        return [normalize_user(u) for u in DEFAULT_USERS]
    
    with open(USERS_FILE, 'r', encoding="utf-8") as f:
        raw_users = json.load(f)
    users = []
    next_id = 1
    changed = False
    for raw_user in raw_users:
        user = normalize_user(raw_user, next_id)
        next_id = max(next_id, int(user.get("id", 0) or 0) + 1)
        changed = changed or user != raw_user
        users.append(user)
    if changed:
        save_users(users)
    return users

def save_users(users):
    """Persist normalized users atomically to Data/users.json."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    normalized_users = [normalize_user(u) for u in users]
    temp_file = USERS_FILE.with_suffix(".json.tmp")
    with open(temp_file, 'w', encoding="utf-8") as f:
        json.dump(normalized_users, f, indent=2)
    try:
        temp_file.replace(USERS_FILE)
    except PermissionError:
        with open(USERS_FILE, 'w', encoding="utf-8") as f:
            json.dump(normalized_users, f, indent=2)
        try:
            temp_file.unlink()
        except OSError:
            pass

def init_users():
    """Ensure the user JSON file exists before the API starts."""
    if not USERS_FILE.exists():
        save_users(DEFAULT_USERS)
        print("✅ Users file created")

import mysql.connector

DATABASE_MODULE_DIR = Path(__file__).parent.parent / "ProcessFiles" / "DatabaseModule"
if str(DATABASE_MODULE_DIR) not in sys.path:
    sys.path.append(str(DATABASE_MODULE_DIR))

from AdvanceDatabase import MySqlDatabase

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}

_DATABASE: MySqlDatabase | None = None


def get_database() -> MySqlDatabase:
    """Return the shared pooled MySQL helper used across the project."""
    global _DATABASE
    if _DATABASE is None:
        _DATABASE = MySqlDatabase(DB_CONFIG, PoolName="PricingSystemPool")
    return _DATABASE


def quote_identifier(name: str) -> str:
    """Quote a MySQL identifier, preserving names with spaces or punctuation."""
    return MySqlDatabase.QuoteIdentifier(name)


def amazon_sales_column_name(header: str) -> str:
    """Convert an Amazon CSV header into the requested DB column name.

    Only spaces are replaced with underscores; other punctuation such as
    ``Hsn/sac`` is preserved and safely quoted when used in SQL.
    """
    name = str(header or "").strip().replace(" ", "_")
    return name or "Unnamed_Column"


def amazon_sales_column_type(header: str) -> str:
    """Infer a pragmatic MySQL type for an Amazon sales CSV column."""
    label = str(header or "").strip().lower()
    if "date" in label:
        return "DATETIME NULL"
    if label == "quantity":
        return "INT NULL"
    numeric_markers = [
        "amount", "gross", "tax", "rate", "basis", "cess", "principal",
        "discount", "total", "invoice amount"
    ]
    if any(marker in label for marker in numeric_markers):
        return "DECIMAL(18,4) NULL"
    if label in {"item description"}:
        return "TEXT"
    return "VARCHAR(255) NULL"


def discover_amazon_sales_headers(kind: str) -> list[str]:
    """Return the ordered union of CSV headers for B2B or B2C sales files."""
    kind_upper = kind.upper()
    headers: list[str] = []
    if not AMAZON_SALES_HISTORY_DIR.exists():
        return headers

    for path in sorted(AMAZON_SALES_HISTORY_DIR.rglob("*.csv")):
        if kind_upper not in path.name.upper():
            continue
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as file:
                row = next(csv.reader(file), [])
        except OSError:
            continue
        for header in row:
            header = str(header or "").strip()
            if header and header not in headers:
                headers.append(header)
    return headers


def get_amazon_sales_schema(kind: str) -> dict[str, Any]:
    """Build the target table name and column metadata for one sales type."""
    normalized_kind = str(kind or "").lower()
    if normalized_kind not in AMAZON_SALES_TABLES:
        raise ValueError("Amazon sales kind must be 'b2b' or 'b2c'")

    seen: dict[str, int] = {}
    columns = []
    for header in discover_amazon_sales_headers(normalized_kind):
        base_col = amazon_sales_column_name(header)
        col_name = base_col
        if col_name in seen:
            seen[col_name] += 1
            suffix = f"_{seen[col_name]}"
            col_name = base_col + suffix
        else:
            seen[col_name] = 1
        columns.append({
            "header": header,
            "name": col_name,
            "type": amazon_sales_column_type(header),
        })

    return {
        "kind": normalized_kind,
        "table": AMAZON_SALES_TABLES[normalized_kind],
        "columns": columns,
    }


def create_amazon_sales_table(cursor, kind: str):
    """Create or extend the Amazon sales table for B2B or B2C history."""
    schema = get_amazon_sales_schema(kind)
    table = schema["table"]
    columns = schema["columns"]
    data_columns_sql = ",\n            ".join(
        f"{quote_identifier(col['name'])} {col['type']}" for col in columns
    )
    if data_columns_sql:
        data_columns_sql += ","

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            {data_columns_sql[:-1] if data_columns_sql.endswith(",") else data_columns_sql}
        )
    """)

    cursor.execute(f"SHOW COLUMNS FROM `{table}`")
    existing_cols = {row[0] for row in cursor.fetchall()}
    for col in columns:
        if col["name"] not in existing_cols:
            cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN {quote_identifier(col['name'])} {col['type']}")
            existing_cols.add(col["name"])

    for index_name, col_name in [
        (f"idx_{table}_sku", "Sku"),
        (f"idx_{table}_order_id", "Order_Id"),
        (f"idx_{table}_invoice_date", "Invoice_Date"),
        (f"idx_{table}_transaction_type", "Transaction_Type"),
    ]:
        if col_name in existing_cols:
            try:
                cursor.execute(f"CREATE INDEX `{index_name}` ON `{table}` ({quote_identifier(col_name)})")
            except Exception:
                pass

    if os.getenv("ENABLE_SALES_DASHBOARD_INDEXES", "").lower() in {"1", "true", "yes"}:
        dashboard_indexes = [
            (f"idx_{table}_tx_date", ["Transaction_Type", "Invoice_Date"]),
            (f"idx_{table}_tx_order", ["Transaction_Type", "Order_Id"]),
            (f"idx_{table}_tx_sku", ["Transaction_Type", "Sku"]),
            (f"idx_{table}_tx_state", ["Transaction_Type", "Ship_To_State"]),
            (f"idx_{table}_tx_city", ["Transaction_Type", "Ship_To_City"]),
            (f"idx_{table}_tx_pincode", ["Transaction_Type", "Ship_To_Postal_Code"]),
            (f"idx_{table}_tx_channel", ["Transaction_Type", "Fulfillment_Channel"]),
            (f"idx_{table}_tx_warehouse", ["Transaction_Type", "Warehouse_Id"]),
            (f"idx_{table}_tx_from_to", ["Transaction_Type", "Ship_From_State", "Ship_To_State"]),
            (f"idx_{table}_invoice_number", ["Invoice_Number"]),
        ]
        for index_name, col_names in dashboard_indexes:
            if all(col_name in existing_cols for col_name in col_names):
                try:
                    cols_sql = ", ".join(quote_identifier(col_name) for col_name in col_names)
                    cursor.execute(f"CREATE INDEX `{index_name}` ON `{table}` ({cols_sql})")
                except Exception:
                    pass


def init_db():
    """Create the application database and all required tables/indexes."""
    init_users()
    
    # First connect without database to create it if it doesn't exist
    temp_config = DB_CONFIG.copy()
    db_name = temp_config.pop("database")
    
    conn = mysql.connector.connect(**temp_config)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    conn.commit()
    cursor.close()
    conn.close()

    # Now use the shared pooled helper for all database-scoped work.
    with get_database().Cursor(Commit=True) as cursor:
        # Create items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sku_code VARCHAR(255),
                item_name TEXT,
                size TEXT,
                category VARCHAR(255),
                location TEXT,
                child_remark TEXT,
                parent_remark TEXT,
                item_type TEXT,
                catalog TEXT,
                cost FLOAT,
                price FLOAT,
                mrp FLOAT,
                up_price FLOAT,
                available_atp INT,
                fba_stock INT,
                fbf_stock INT,
                sjit_stock INT,
                updated TEXT,
                INDEX idx_sku (sku_code),
                INDEX idx_category (category)
            )
        """)

        try:
            cursor.execute("ALTER TABLE stock_items DROP COLUMN cost_into_percent")
        except Exception:
            pass

        for index_sql in [
            "CREATE INDEX idx_stock_items_style_id ON stock_items (item_name(100))",
            "CREATE INDEX idx_stock_items_size ON stock_items (size(50))",
            "CREATE INDEX idx_stock_items_type ON stock_items (item_type(50))",
        ]:
            try:
                cursor.execute(index_sql)
            except Exception:
                pass
        
        # Create stock_update table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_update (
                id INT AUTO_INCREMENT PRIMARY KEY,
                master_sku VARCHAR(255) UNIQUE,
                uniware_stock INT DEFAULT 0,
                fba_stock INT DEFAULT 0,
                fbf_stock INT DEFAULT 0,
                sjit_stock INT DEFAULT 0,
                INDEX idx_master_sku (master_sku)
            )
        """)

        # Create catalog_pricing table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS catalog_pricing (
                id INT AUTO_INCREMENT PRIMARY KEY,
                master_sku VARCHAR(255) UNIQUE,
                launch_date TEXT,
                catalog_name TEXT,
                cost FLOAT DEFAULT 0.0,
                wholesale_price FLOAT DEFAULT 0.0,
                up_price FLOAT DEFAULT 0.0,
                mrp FLOAT DEFAULT 0.0,
                INDEX idx_pricing_sku (master_sku)
            )
        """)

        cursor.execute("SHOW COLUMNS FROM catalog_pricing")
        catalog_cols = {row[0] for row in cursor.fetchall()}
        catalog_renames = [
            ("Master SKU", "master_sku", "VARCHAR(255)"),
            ("Launch Date", "launch_date", "TEXT"),
            ("Catalog Name", "catalog_name", "TEXT"),
            ("Cost", "cost", "TEXT"),
            ("Wholesale Price", "wholesale_price", "TEXT"),
            ("Up Price", "up_price", "TEXT"),
            ("MRP", "mrp", "TEXT"),
        ]
        for old_col, new_col, col_type in catalog_renames:
            if old_col in catalog_cols and new_col not in catalog_cols:
                try:
                    cursor.execute(
                        f"ALTER TABLE catalog_pricing CHANGE COLUMN `{old_col}` `{new_col}` {col_type}"
                    )
                    catalog_cols.remove(old_col)
                    catalog_cols.add(new_col)
                except Exception:
                    pass

        for col_name, col_type in [
            ("master_sku", "VARCHAR(255)"),
            ("launch_date", "TEXT"),
            ("catalog_name", "TEXT"),
            ("cost", "FLOAT DEFAULT 0.0"),
            ("wholesale_price", "FLOAT DEFAULT 0.0"),
            ("up_price", "FLOAT DEFAULT 0.0"),
            ("mrp", "FLOAT DEFAULT 0.0"),
        ]:
            if col_name not in catalog_cols:
                try:
                    cursor.execute(f"ALTER TABLE catalog_pricing ADD COLUMN `{col_name}` {col_type}")
                    catalog_cols.add(col_name)
                except Exception:
                    pass

        try:
            cursor.execute("CREATE INDEX idx_pricing_sku ON catalog_pricing (master_sku)")
        except Exception:
            pass

        try:
            cursor.execute("ALTER TABLE catalog_pricing DROP COLUMN cost_into_percent")
        except Exception:
            pass

        # Create platform-wise cost percent table from item_master.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cost_into_percent (
                id INT AUTO_INCREMENT PRIMARY KEY,
                master_sku VARCHAR(255),
                style_id VARCHAR(255),
                Platform VARCHAR(100) DEFAULT 'Amazon',
                Cost_Into_Percent FLOAT DEFAULT 23.0,
                INDEX idx_cost_percent_sku (master_sku),
                INDEX idx_cost_percent_platform (Platform)
            )
        """)

        cursor.execute("SHOW TABLES LIKE 'item_master'")
        item_master_exists = cursor.fetchone() is not None
        cursor.execute("SELECT COUNT(*) FROM cost_into_percent")
        cost_percent_count = cursor.fetchone()[0]
        if item_master_exists and cost_percent_count == 0:
            cursor.execute("""
                INSERT INTO cost_into_percent
                    (master_sku, style_id, Platform, Cost_Into_Percent)
                SELECT
                    `Master SKU`,
                    `Style ID / Parent SKU`,
                    'Amazon',
                    23
                FROM item_master
            """)

        # Create amazon_pricing_results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS amazon_pricing_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                master_sku VARCHAR(255),
                original_category VARCHAR(255),
                amazon_cat VARCHAR(255),
                remark TEXT,
                cost FLOAT DEFAULT 0.0,
                mrp FLOAT DEFAULT 0.0,
                uniware INT DEFAULT 0,
                fba INT DEFAULT 0,
                sjit INT DEFAULT 0,
                fbf INT DEFAULT 0,
                launch_date TEXT,
                loc TEXT,
                cost_into_percent FLOAT DEFAULT 0.0,
                cost_after_percent FLOAT DEFAULT 0.0,
                return_charge FLOAT DEFAULT 0.0,
                gst_on_return FLOAT DEFAULT 0.0,
                final_tp FLOAT DEFAULT 0.0,
                required_selling_price FLOAT DEFAULT 0.0,
                selected_price_range VARCHAR(100),
                selected_fixed_fee_range VARCHAR(100),
                commission_percent FLOAT DEFAULT 0.0,
                commission_amount FLOAT DEFAULT 0.0,
                fixed_closing_fee FLOAT DEFAULT 0.0,
                fba_pick_pack FLOAT DEFAULT 0.0,
                technology_fee FLOAT DEFAULT 0.0,
                full_shipping_fee FLOAT DEFAULT 0.0,
                whf_percent_on_shipping FLOAT DEFAULT 0.0,
                shipping_fee_charged FLOAT DEFAULT 0.0,
                total_charges FLOAT DEFAULT 0.0,
                final_value_after_charges FLOAT DEFAULT 0.0,
                old_daily_sp FLOAT DEFAULT 0.0,
                old_deal_sp FLOAT DEFAULT 0.0,
                sett_acc_panel FLOAT DEFAULT 0.0,
                net_profit_on_sp FLOAT DEFAULT 0.0,
                net_profit_percent_on_sp FLOAT DEFAULT 0.0,
                net_profit_percent_on_tp FLOAT DEFAULT 0.0,
                INDEX idx_amazon_sku (master_sku)
            )
        """)

        # Amazon pricing must preserve duplicate SKU rows from item master.
        # Older databases created master_sku as UNIQUE, which made KPIs too low.
        try:
            cursor.execute("ALTER TABLE amazon_pricing_results DROP INDEX master_sku")
        except Exception:
            pass

        try:
            cursor.execute("ALTER TABLE amazon_pricing_results ADD COLUMN old_deal_sp FLOAT DEFAULT 0.0 AFTER old_daily_sp")
        except Exception:
            pass

        # Amazon sales history tables are populated from Data/AmazonData/AmazonSalesHistory.
        create_amazon_sales_table(cursor, "b2b")
        create_amazon_sales_table(cursor, "b2c")

        for ddl in [
            "ALTER TABLE stock_items ADD COLUMN child_remark TEXT AFTER location",
            "ALTER TABLE stock_items ADD COLUMN parent_remark TEXT AFTER child_remark",
            "ALTER TABLE stock_items ADD COLUMN item_type TEXT AFTER parent_remark",
        ]:
            try:
                cursor.execute(ddl)
            except Exception:
                pass

def get_db():
    """Return a pooled MySQL connection from the shared database helper."""
    return get_database().GetConnection()

# For update_item_csv function referenced in items.py
async def update_item_csv(item_id, user):
    """Legacy compatibility placeholder; item updates now flow through MySQL."""
    raise NotImplementedError("Direct item update not fully implemented in pipeline mode yet.")
