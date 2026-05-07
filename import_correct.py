import sys
db_path = 'D:/VatsalFiles/PricingModule/pricing_management_system/pricing_system/data/pricing.db'
import sqlite3
import csv
from datetime import datetime

with open('D:/VatsalFiles/PricingModule/Data/ItemMaster_selected.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    rows = list(reader)

if not rows:
    print("Empty CSV")
    exit(1)

headers = [h.strip() for h in rows[0]]
print(f"Headers: {headers}")

data_rows = rows[1:]

conn = sqlite3.connect(db_path)
new_rows = updated_rows = skipped = 0
import_batch = datetime.now().strftime('%Y%m%d_%H%M%S')

def safe_int(val):
    try: return int(float(str(val).strip() or 0))
    except: return 0

for row in data_rows:
    if len(row) < 2:
        skipped += 1
        continue
    
    # Map by position
    sku = row[0].strip() if row[0] else ''
    if not sku:
        skipped += 1
        continue
    
    # Handle short rows - pad to header length
    row_padded = row + [''] * (len(headers) - len(row))
    
    data = {
        'status': 'Active',
        'sku_code': sku,
        'item_name': row_padded[1].strip() if len(row_padded) > 1 else '',
        'size': row_padded[2].strip() if len(row_padded) > 2 else '',
        'category': row_padded[3].strip() if len(row_padded) > 3 else '',
        'location': row_padded[4].strip() if len(row_padded) > 4 else '',
        'cost': safe_int(row_padded[5]) if len(row_padded) > 5 else 0,
        'price': safe_int(row_padded[6]) if len(row_padded) > 6 else 0,
        'catalog': row_padded[7].strip() if len(row_padded) > 7 else '',
        'mrp': safe_int(row_padded[8]) if len(row_padded) > 8 else 0,
        'available_atp': safe_int(row_padded[9]) if len(row_padded) > 9 else 0,
        'fba_stock': safe_int(row_padded[10]) if len(row_padded) > 10 else 0,
        'fbf_stock': safe_int(row_padded[11]) if len(row_padded) > 11 else 0,
        'sjit_stock': safe_int(row_padded[12]) if len(row_padded) > 12 else 0,
        'updated': row_padded[13].strip() if len(row_padded) > 13 else '',
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

# Verify
cat_count = conn.execute('SELECT COUNT(*) FROM stock_items WHERE category IS NOT NULL AND category != ""').fetchone()[0]
print(f'Import: {new_rows} new, {updated_rows} updated, {skipped} skipped')
print(f'Items with Category: {cat_count}')

# Sample
results = conn.execute('SELECT sku_code, category, location, available_atp FROM stock_items WHERE sku_code IN ("JOPLAI1001D", "JOPLCS107-L")').fetchall()
for r in results:
    print(r)

conn.close()