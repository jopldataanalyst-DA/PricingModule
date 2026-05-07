import csv
import io

with open('D:/VatsalFiles/PricingModule/Data/ItemMaster_selected.csv', 'r', encoding='utf-8-sig') as f:
    content = f.read()
    lines = content.splitlines()

header_idx = 0
for i, line in enumerate(lines):
    if 'Master SKU' in line:
        header_idx = i
        break

reader = csv.DictReader(io.StringIO('\n'.join(lines[header_idx+1:])))

# Show first 5 rows keys
for i, row in enumerate(reader):
    if i < 3:
        print(f'Row {i}: {row.get("Master SKU")}')
        print(f'  Keys: {list(row.keys())[:5]}')
    else:
        break