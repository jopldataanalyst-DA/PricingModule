DB_PATH = 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db'
import sqlite3
import csv
import io
from datetime import datetime

# Read ItemMaster CSV
with open('D:/VatsalFiles/PricingModule/Data/ItemMaster_selected.csv', 'r', encoding='utf-8-sig') as f:
    content = f.read()
    lines = content.splitlines()

header_idx = 0
for i, line in enumerate(lines):
    if 'Master SKU' in line:
        header_idx = i
        break

reader = csv.DictReader(io.StringIO('\n'.join(lines[header_idx+1:])))

conn = sqlite3.connect(DB_PATH)
new_rows = updated_rows = 0
import_batch = datetime.now().strftime('%Y%m%d_%H%M%S')

def safe_int(val):
    try: return int(float(str(val).strip() or 0))
    except: return 0

for row in reader:
    sku = row.get('Master SKU', '').strip()
    if not sku:
        continue
    
    data = {
        'status': 'Active',
        'sku_code': sku,
        'item_name': row.get('Style ID / Parent SKU', '').strip(),
        'size': row.get('Size', '').strip(),
        'category': row.get('Category', '').strip(),
        'location': row.get('Location', '').strip(),
        'cost': safe_int(row.get('Cost', 0)),
        'price': safe_int(row.get('Wholesale Price', 0)),
        'catalog': row.get('Catalog Name', '').strip(),
        'mrp': safe_int(row.get('Up Price', 0)),
        'available_atp': safe_int(row.get('Uniware Stock', 0)),
        'fba_stock': safe_int(row.get('FBA', 0)),
        'fbf_stock': safe_int(row.get('FBF', 0)),
        'sjit_stock': safe_int(row.get('SJIT', 0)),
        'updated': row.get('Launch Date', '').strip(),
        'import_batch': import_batch,
    }
    
    existing = conn.execute('SELECT id FROM stock_items WHERE sku_code=?', (sku,)).fetchone()
    if existing:
        set_clause = ', '.join([f'{k}=?' for k in data if k != 'sku_code'])
        vals = [v for k, v in data.items() if k != 'sku_code'] + [sku]
        conn.execute(f'UPDATE stock_items SET {set_clause}, updated_at=datetime("now") WHERE sku_code=?', vals)
        updated_rows += 1
    else:
        cols = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        conn.execute(f'INSERT INTO stock_items ({cols}) VALUES ({placeholders})', list(data.values()))
        new_rows += 1

conn.commit()
print(f'Import complete: {new_rows} new, {updated_rows} updated')
print(f'Total rows in DB: {conn.execute("SELECT COUNT(*) FROM stock_items").fetchone()[0]}')
conn.close()