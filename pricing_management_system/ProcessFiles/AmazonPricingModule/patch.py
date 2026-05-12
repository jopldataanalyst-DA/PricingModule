"""One-time source patch script for AmazonPricing.py.

Use case:
    Replaces an older DuckDB/MySQL stock-loading implementation with a direct
    mysql.connector implementation. This is historical maintenance tooling and
    should not be run during normal application startup.
"""

import re

with open('d:/VatsalFiles/PricingModule/pricing_management_system/ProcessFiles/AmazonPricingModule/AmazonPricing.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_func = '''def LoadStockData():
    Con = duckdb.connect()

    Con.execute("INSTALL mysql")
    Con.execute("LOAD mysql")

    MysqlConn = (
        f"host={DbConfig['host']} "
        f"user={DbConfig['user']} "
        f"password={DbConfig['password']} "
        f"database={DbConfig['database']}"
    )

    Con.execute(f"ATTACH '{MysqlConn}' AS mysql_db (TYPE mysql)")

    ArrowTable = Con.execute(f"""
        SELECT
            sku_code AS Master_SKU,
            category AS Category,
            parent_remark AS Remark,
            cost AS Cost,
            mrp AS MRP,
            available_atp AS Uniware,
            fba_stock AS FBA,
            sjit_stock AS Sjit,
            fbf_stock AS FBF,
            location AS LOC,
            price AS Current_Price,
            updated AS Launch_Date
        FROM mysql_db.{TableName}
    """).arrow()

    Con.close()

    return pl.from_arrow(ArrowTable)'''

new_func = '''def LoadStockData():
    import mysql.connector
    MysqlConn = mysql.connector.connect(**DbConfig)
    cursor = MysqlConn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT
            sku_code AS Master_SKU,
            category AS Category,
            parent_remark AS Remark,
            cost AS Cost,
            mrp AS MRP,
            available_atp AS Uniware,
            fba_stock AS FBA,
            sjit_stock AS Sjit,
            fbf_stock AS FBF,
            location AS LOC,
            price AS Current_Price,
            updated AS Launch_Date
        FROM {TableName}
    """)
    rows = cursor.fetchall()
    cursor.close()
    MysqlConn.close()

    if not rows:
        return pl.DataFrame(schema={
            "Master_SKU": pl.Utf8,
            "Category": pl.Utf8,
            "Remark": pl.Utf8,
            "Cost": pl.Float64,
            "MRP": pl.Float64,
            "Uniware": pl.Float64,
            "FBA": pl.Float64,
            "Sjit": pl.Float64,
            "FBF": pl.Float64,
            "LOC": pl.Utf8,
            "Current_Price": pl.Float64,
            "Launch_Date": pl.Utf8
        })
    return pl.from_dicts(rows)'''

content = content.replace(old_func, new_func)

with open('d:/VatsalFiles/PricingModule/pricing_management_system/ProcessFiles/AmazonPricingModule/AmazonPricing.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Replaced')
