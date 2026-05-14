"""Import Amazon sales history CSV files into MySQL.

Use case:
    Reads B2B and B2C monthly Amazon MTR sales CSVs from
    Data/AmazonData/AmazonSalesHistory, creates matching database rows in
    amazon_sales_b2b and amazon_sales_b2c, and keeps source-file metadata so
    rows can be traced back to the original report.
"""

import argparse
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from database import (
    AMAZON_SALES_HISTORY_DIR,
    get_database,
    get_amazon_sales_schema,
    init_db,
    quote_identifier,
)


DATE_COLUMNS = {
    "Invoice_Date",
    "Shipment_Date",
    "Order_Date",
    "Credit_Note_Date",
    "Irn_Date",
}


def iter_sales_files(kind: str) -> list[Path]:
    """Find all B2B or B2C CSV files in the sales history folder."""
    kind_upper = kind.upper()
    if not AMAZON_SALES_HISTORY_DIR.exists():
        return []
    return [
        path for path in sorted(AMAZON_SALES_HISTORY_DIR.rglob("*.csv"))
        if kind_upper in path.name.upper()
    ]


def parse_date(value: Any):
    """Parse the date formats observed in Amazon MTR reports."""
    value = str(value or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def parse_number(value: Any):
    """Parse numeric CSV text into Decimal while treating blanks as NULL."""
    value = str(value or "").strip().replace(",", "")
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def normalize_value(value: Any, col_type: str, col_name: str):
    """Convert one CSV cell into the Python value expected by MySQL."""
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    if col_name in DATE_COLUMNS or col_type.startswith("DATETIME"):
        return parse_date(value)
    if col_type.startswith("INT"):
        parsed = parse_number(value)
        return int(parsed) if parsed is not None else None
    if col_type.startswith("DECIMAL"):
        return parse_number(value)
    return value


def import_amazon_sales_history(kind: str, refresh: bool = False, batch_size: int = 1000) -> dict[str, int]:
    """Import one sales type into its target table.

    Args:
        kind: Either ``b2b`` or ``b2c``.
        refresh: When true, truncate the target table before loading.
        batch_size: Number of rows inserted per executemany call.

    Returns:
        Counts for files scanned, rows processed, and rows inserted.
    """
    init_db()
    schema = get_amazon_sales_schema(kind)
    table = schema["table"]
    columns = schema["columns"]
    insert_cols = [col["name"] for col in columns]
    placeholders = ", ".join(["%s"] * len(insert_cols))
    col_sql = ", ".join(quote_identifier(col) for col in insert_cols)
    insert_sql = f"INSERT IGNORE INTO {quote_identifier(table)} ({col_sql}) VALUES ({placeholders})"

    files = iter_sales_files(kind)
    db = get_database()
    inserted = 0
    processed = 0
    if refresh:
        db.TruncateTable(table)

    for path in files:
        relative_file = str(path.relative_to(AMAZON_SALES_HISTORY_DIR))
        source_year = path.parent.name
        batch = []
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            for row_number, row in enumerate(reader, start=2):
                values = []
                for col in columns:
                    values.append(normalize_value(row.get(col["header"]), col["type"], col["name"]))
                batch.append(tuple(values))
                processed += 1
                if len(batch) >= batch_size:
                    inserted += db.ExecuteMany(insert_sql, batch, BatchSize=batch_size)
                    batch.clear()
            if batch:
                inserted += db.ExecuteMany(insert_sql, batch, BatchSize=batch_size)

    return {"files": len(files), "processed_rows": processed, "inserted_rows": inserted}


def import_all_amazon_sales_history(refresh: bool = False) -> dict[str, dict[str, int]]:
    """Import both B2B and B2C sales history tables."""
    return {
        "b2b": import_amazon_sales_history("b2b", refresh=refresh),
        "b2c": import_amazon_sales_history("b2c", refresh=refresh),
    }


def main():
    """Command-line entrypoint for manual sales-history imports."""
    parser = argparse.ArgumentParser(description="Import Amazon sales history CSVs into MySQL.")
    parser.add_argument("--kind", choices=["b2b", "b2c", "all"], default="all")
    parser.add_argument("--refresh", action="store_true", help="Truncate target table(s) before importing.")
    args = parser.parse_args()

    if args.kind == "all":
        result = import_all_amazon_sales_history(refresh=args.refresh)
    else:
        result = {args.kind: import_amazon_sales_history(args.kind, refresh=args.refresh)}
    print(result)


if __name__ == "__main__":
    main()

