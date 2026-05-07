import mysql.connector
import pandas as pd
import os
import re


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}


def clean_column_name(col):
    col = str(col).strip()
    col = re.sub(r"`", "", col)
    return col


def read_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)

    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)

    else:
        raise ValueError("Only CSV and Excel files are supported")


def table_exists(cursor, table_name):
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return cursor.fetchone() is not None


def create_table(cursor, table_name, df):
    column_defs = ["id INT AUTO_INCREMENT PRIMARY KEY"]

    for col in df.columns:
        column_defs.append(f"`{col}` TEXT")

    create_query = f"""
    CREATE TABLE `{table_name}` (
        {", ".join(column_defs)}
    )
    """

    cursor.execute(create_query)


def insert_data(cursor, table_name, df):
    csv_cols = ", ".join([f"`{col}`" for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))

    insert_query = f"""
    INSERT INTO `{table_name}` ({csv_cols})
    VALUES ({placeholders})
    """

    data = df.where(pd.notnull(df), None).values.tolist()

    cursor.executemany(insert_query, data)


def upload_table(file_path, table_name, action="append"):
    """
    action:
        append  -> add data
        replace -> drop and recreate table
    """

    action = action.lower()

    if action not in ["append", "replace"]:
        raise ValueError("action must be 'append' or 'replace'")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        df = read_file(file_path)
        df.columns = [clean_column_name(col) for col in df.columns]

        exists = table_exists(cursor, table_name)

        if exists:

            if action == "replace":
                cursor.execute(f"DROP TABLE `{table_name}`")
                create_table(cursor, table_name, df)
                insert_data(cursor, table_name, df)

            elif action == "append":
                insert_data(cursor, table_name, df)

        else:
            create_table(cursor, table_name, df)
            insert_data(cursor, table_name, df)

        conn.commit()

        print(f"Upload completed successfully")
        print(f"Inserted {len(df)} rows into '{table_name}'")

    except Exception as e:
        conn.rollback()
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()


# =========================
# Example Usage
# =========================

upload_table(
    file_path=r"D:\VatsalFiles\PricingModule\Data\PricingModuleData.csv",
    table_name="Pricing_Module_Data",
    action="replace"   # append / replace
)