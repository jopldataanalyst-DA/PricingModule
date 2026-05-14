"""Generic CSV/Excel to MySQL table loader.

Use case:
    Utility script for quickly uploading a spreadsheet into MySQL either by
    appending rows or replacing the target table. It is useful for ad-hoc data
    staging, not part of the FastAPI request path.
"""

import pandas as pd
import os
import re
from AdvanceDatabase import MySqlDatabase


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}


def get_database() -> MySqlDatabase:
    return MySqlDatabase(DB_CONFIG, PoolName="SqlDataUpdaterPool")


def clean_column_name(col):
    """Strip unsafe MySQL backticks from a source column name."""
    col = str(col).strip()
    col = re.sub(r"`", "", col)
    return col


def read_file(file_path):
    """Read CSV/XLS/XLSX data into a pandas DataFrame."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)

    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)

    else:
        raise ValueError("Only CSV and Excel files are supported")


def table_exists(cursor, table_name):
    """Return true if a MySQL table already exists."""
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return cursor.fetchone() is not None


def create_table(cursor, table_name, df):
    """Create a simple TEXT-column table matching DataFrame columns."""
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
    """Insert all DataFrame rows into the target MySQL table."""
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
    Upload one spreadsheet into MySQL.

    action:
        append  -> add data
        replace -> drop and recreate table
    """

    action = action.lower()

    if action not in ["append", "replace"]:
        raise ValueError("action must be 'append' or 'replace'")

    try:
        db = get_database()
        with db.Cursor(Commit=True) as cursor:
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

        print(f"Upload completed successfully")
        print(f"Inserted {len(df)} rows into '{table_name}'")

    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    upload_table(
        file_path=r"D:\VatsalFiles\PricingModule\Data\ItemMaster.csv",
        table_name="item_master",
        action="replace"   # append / replace
    )
