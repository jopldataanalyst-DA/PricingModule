import requests
import csv
import io

URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv"

print("Fetching from Google Sheet...")
response = requests.get(URL)
content = response.text
lines = content.splitlines()

print(f"Got {len(lines)} lines")

# Parse the data
# Format: SKU, Uniware stock, FBA SKU, FBA qty, FBF SKU, FBF qty, SJIT SKU, SJIT qty
stock_data = {}

for i, line in enumerate(lines[2:], start=2):  # Skip header rows
    if not line.strip():
        continue
    
    parts = line.split(',')
    if len(parts) < 2:
        continue
    
    # First column is SKU, second is Uniware stock
    sku = parts[0].strip()
    uni_stock = parts[1].strip() if len(parts) > 1 else '0'
    
    if not sku:
        continue
    
    # Clean Uniware stock (remove quotes, commas)
    uni_stock = uni_stock.replace('"', '').replace(',', '').strip()
    try:
        uni_val = int(uni_stock) if uni_stock else 0
    except:
        uni_val = 0
    
    stock_data[sku] = {
        'sku_code': sku,
        'available_atp': uni_val,
        'fba_stock': 0,
        'fbf_stock': 0,
        'sjit_stock': 0
    }

# Now parse FBA, FBF, SJIT columns
# These are in alternating columns
for i, line in enumerate(lines[2:], start=2):
    if not line.strip():
        continue
    
    parts = line.split(',')
    if len(parts) < 5:
        continue
    
    # FBA is in columns 3-4 (index 2-3)
    if len(parts) > 3:
        fba_sku = parts[2].strip().replace('"', '')
        fba_qty = parts[3].strip().replace('"', '').replace(',', '').strip()
        if fba_sku and fba_sku != 'uni' and fba_sku != 'FBA':
            try:
                stock_data[fba_sku] = stock_data.get(fba_sku, {'sku_code': fba_sku, 'available_atp': 0, 'fba_stock': 0, 'fbf_stock': 0, 'sjit_stock': 0})
                stock_data[fba_sku]['fba_stock'] = int(fba_qty) if fba_qty else 0
            except:
                pass
    
    # FBF is in columns 5-6 (index 4-5)
    if len(parts) > 5:
        fbf_sku = parts[4].strip().replace('"', '')
        fbf_qty = parts[5].strip().replace('"', '').replace(',', '').strip()
        if fbf_sku and fbf_sku != 'STOCK':
            try:
                stock_data[fbf_sku] = stock_data.get(fbf_sku, {'sku_code': fbf_sku, 'available_atp': 0, 'fba_stock': 0, 'fbf_stock': 0, 'sjit_stock': 0})
                stock_data[fbf_sku]['fbf_stock'] = int(fbf_qty) if fbf_qty else 0
            except:
                pass
    
    # SJIT is in columns 7-8 (index 6-7)
    if len(parts) > 7:
        sjit_sku = parts[6].strip().replace('"', '')
        sjit_qty = parts[7].strip().replace('"', '').replace(',', '').strip()
        if sjit_sku and sjit_sku != 'STOCK':
            try:
                stock_data[sjit_sku] = stock_data.get(sjit_sku, {'sku_code': sjit_sku, 'available_atp': 0, 'fba_stock': 0, 'fbf_stock': 0, 'sjit_stock': 0})
                stock_data[sjit_sku]['sjit_stock'] = int(sjit_qty) if sjit_qty else 0
            except:
                pass

print(f"Parsed {len(stock_data)} items")

# Read existing ItemMaster CSV
input_file = 'D:/VatsalFiles/PricingModule/Data/ItemMaster_selected.csv'
output_file = 'D:/VatsalFiles/PricingModule/Data/ItemMaster_selected_updated.csv'

updated = 0
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    rows = list(reader)

# Update stock values
for i, row in enumerate(rows):
    if i == 0:  # Header
        continue
    if len(row) < 1:
        continue
    
    sku = row[0].strip()
    if sku in stock_data:
        # Uniware Stock is column 9 (index 9)
        # FBA is column 10 (index 10)
        # FBF is column 11 (index 11)
        # SJIT is column 12 (index 12)
        
        while len(row) < 13:
            row.append('')
        
        row[9] = str(stock_data[sku]['available_atp'])
        row[10] = str(stock_data[sku]['fba_stock'])
        row[11] = str(stock_data[sku]['fbf_stock'])
        row[12] = str(stock_data[sku]['sjit_stock'])
        updated += 1

print(f"Updated {updated} items")

# Write to output
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(rows)

print(f"Saved to {output_file}")
print(f"\nSample updates:")
for i, (sku, data) in enumerate(list(stock_data.items())[:5]):
    print(f"  {sku}: uni={data['available_atp']}, fba={data['fba_stock']}, fbf={data['fbf_stock']}, sjit={data['sjit_stock']}")
