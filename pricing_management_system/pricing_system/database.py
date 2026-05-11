import json
from pathlib import Path
import hashlib
from typing import Any

DATA_DIR = Path(__file__).parent.parent.parent / "Data"

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

USERS_FILE = DATA_DIR / "users.json"

AMAZON_PRICING_COLUMNS = [
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

AMAZON_PRICING_DEFAULT_VISIBLE = [
    "master_sku", "item_name", "original_category", "amazon_cat", "remark",
    "cost", "mrp", "uniware", "fba", "sjit", "fbf",
    "launch_date", "loc", "cost_into_percent", "cost_after_percent",
    "return_charge", "gst_on_return", "final_tp", "required_selling_price",
    "sett_acc_panel", "net_profit_on_sp", "net_profit_percent_on_sp",
    "net_profit_percent_on_tp", "old_daily_sp", "old_deal_sp"
]

DEFAULT_COLUMN_PERMISSIONS = {
    "item_master": {
        "visible": [
            "sku_code", "item_name", "size", "category", "location",
            "child_remark", "parent_remark", "item_type",
            "cost", "price", "catalog", "mrp", "up_price", "cost_into_percent",
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
        "visible": ["master_sku", "item_name", "original_category", "amazon_cat", "remark", "cost", "mrp", "uniware", "fba", "sjit", "fbf"],
        "editable": []
    }
}

DEFAULT_USERS = [
    {"id": 1, "username": "admin", "password": hash_password("admin123"), "special_password": "", "role": "admin", "allowed_pages": ["item_master", "amazon_pricing", "admin", "logs", "import"], "column_permissions": ADMIN_COLUMN_PERMISSIONS, "is_active": True},
    {"id": 2, "username": "vikesh", "password": hash_password("vikesh123"), "special_password": hash_password("vikesh123"), "role": "viewer", "allowed_pages": ["item_master"], "column_permissions": DEFAULT_COLUMN_PERMISSIONS, "is_active": True},
    {"id": 3, "username": "hitesh", "password": hash_password("hitesh123"), "special_password": hash_password("hitesh123"), "role": "restricted", "allowed_pages": ["item_master"], "column_permissions": RESTRICTED_COLUMN_PERMISSIONS, "is_active": True},
]


def _as_list(value: Any, fallback: list[str]) -> list[str]:
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
    role = str(user.get("role") or "viewer").lower()
    if role == "admin":
        default_perms = ADMIN_COLUMN_PERMISSIONS
    elif role == "restricted":
        default_perms = RESTRICTED_COLUMN_PERMISSIONS
    else:
        default_perms = DEFAULT_COLUMN_PERMISSIONS
    allowed_default = ["item_master", "amazon_pricing", "admin", "logs", "import"] if role == "admin" else ["item_master"]
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
    normalized["column_permissions"] = {"item_master": {"visible": visible, "editable": editable}}

    amazon_pricing = column_permissions.get("amazon_pricing") if isinstance(column_permissions, dict) else None
    if not isinstance(amazon_pricing, dict):
        amazon_pricing = default_perms.get("amazon_pricing", {"visible": AMAZON_PRICING_DEFAULT_VISIBLE, "editable": []})
    ap_visible = _as_list(amazon_pricing.get("visible"), default_perms.get("amazon_pricing", {"visible": AMAZON_PRICING_DEFAULT_VISIBLE, "editable": []})["visible"])
    ap_editable = _as_list(amazon_pricing.get("editable"), default_perms.get("amazon_pricing", {"visible": AMAZON_PRICING_DEFAULT_VISIBLE, "editable": []})["editable"])
    normalized["column_permissions"]["amazon_pricing"] = {"visible": ap_visible, "editable": ap_editable}
    return normalized

def load_users():
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
    if not USERS_FILE.exists():
        save_users(DEFAULT_USERS)
        print("✅ Users file created")

import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}

def init_db():
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

    # Now connect with database to create tables
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
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
            cost_into_percent FLOAT DEFAULT 23.0,
            available_atp INT,
            fba_stock INT,
            fbf_stock INT,
            sjit_stock INT,
            updated TEXT,
            INDEX idx_sku (sku_code),
            INDEX idx_category (category)
        )
    """)
    
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

    for ddl in [
        "ALTER TABLE stock_items ADD COLUMN child_remark TEXT AFTER location",
        "ALTER TABLE stock_items ADD COLUMN parent_remark TEXT AFTER child_remark",
        "ALTER TABLE stock_items ADD COLUMN item_type TEXT AFTER parent_remark",
    ]:
        try:
            cursor.execute(ddl)
        except Exception:
            pass

    conn.commit()
    cursor.close()
    conn.close()

def get_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    return conn

# For update_item_csv function referenced in items.py
async def update_item_csv(item_id, user):
    raise NotImplementedError("Direct item update not fully implemented in pipeline mode yet.")
