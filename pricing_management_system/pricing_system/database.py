import json
from pathlib import Path
import hashlib

DATA_DIR = Path(__file__).parent.parent.parent / "Data"

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

USERS_FILE = DATA_DIR / "users.json"

DEFAULT_USERS = [
    {"username": "admin", "password": hash_password("admin123"), "role": "admin", "allowed_pages": ["item_master", "admin", "logs", "import"]},
    {"username": "vikesh", "password": hash_password("vikesh123"), "role": "viewer", "allowed_pages": ["item_master"]},
    {"username": "hitesh", "password": hash_password("hitesh123"), "role": "restricted", "allowed_pages": ["item_master"]},
]

def load_users():
    if not USERS_FILE.exists():
        save_users(DEFAULT_USERS)
        return DEFAULT_USERS
    
    with open(USERS_FILE, 'r') as f:
        return json.load(f)

def save_users(users):
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=2)

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
            sku_code VARCHAR(255) UNIQUE,
            item_name TEXT,
            size TEXT,
            category VARCHAR(255),
            location TEXT,
            catalog TEXT,
            cost FLOAT,
            price FLOAT,
            mrp FLOAT,
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
            INDEX idx_pricing_sku (master_sku)
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def get_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    return conn

# For update_item_csv function referenced in items.py
async def update_item_csv(item_id, user):
    raise NotImplementedError("Direct item update not fully implemented in pipeline mode yet.")