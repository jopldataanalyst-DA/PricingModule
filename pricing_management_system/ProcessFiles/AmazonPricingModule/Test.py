import polars as pl
import mysql.connector
from mysql.connector import pooling


# ============================================================
# DATABASE CONFIG
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module",
}


# ============================================================
# CONNECTION POOL
# ============================================================

Pool = pooling.MySQLConnectionPool(
    pool_name="PricingModulePool",
    pool_size=10,
    pool_reset_session=True,
    **DB_CONFIG,
)


def GetConnection():
    return Pool.get_connection()


# ============================================================
# BASIC READ FUNCTIONS
# ============================================================

def ReadTable(TableName: str) -> pl.DataFrame:
    Query = f"SELECT * FROM `{TableName}`"
    return ExecuteReadQuery(Query)


def ReadLimit(TableName: str, Limit: int = 10) -> pl.DataFrame:
    Query = f"SELECT * FROM `{TableName}` LIMIT %s"
    return ExecuteReadQuery(Query, (Limit,))


def ReadWhere(
    TableName: str,
    WhereCondition: str,
    Params: tuple | None = None,
) -> pl.DataFrame:
    Query = f"SELECT * FROM `{TableName}` WHERE {WhereCondition}"
    return ExecuteReadQuery(Query, Params)


def ExecuteReadQuery(
    Query: str,
    Params: tuple | None = None,
) -> pl.DataFrame:
    Conn = GetConnection()
    Cursor = Conn.cursor(dictionary=True)

    try:
        Cursor.execute(Query, Params or ())
        Data = Cursor.fetchall()
        return pl.DataFrame(Data)

    finally:
        Cursor.close()
        Conn.close()


# ============================================================
# BASIC WRITE FUNCTIONS
# ============================================================

def WriteTable(
    DataFrame: pl.DataFrame,
    TableName: str,
    BatchSize: int = 5000,
) -> None:
    if DataFrame.is_empty():
        return

    Columns = DataFrame.columns
    ColumnString = ", ".join(f"`{Col}`" for Col in Columns)
    PlaceholderString = ", ".join(["%s"] * len(Columns))

    Query = f"""
        INSERT INTO `{TableName}`
        ({ColumnString})
        VALUES ({PlaceholderString})
    """

    Rows = DataFrame.rows()

    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        for Start in range(0, len(Rows), BatchSize):
            Batch = Rows[Start:Start + BatchSize]
            Cursor.executemany(Query, Batch)

        Conn.commit()

    except Exception:
        Conn.rollback()
        raise

    finally:
        Cursor.close()
        Conn.close()


def InsertRow(TableName: str, Data: dict) -> None:
    Columns = list(Data.keys())
    Values = tuple(Data.values())

    ColumnString = ", ".join(f"`{Col}`" for Col in Columns)
    PlaceholderString = ", ".join(["%s"] * len(Columns))

    Query = f"""
        INSERT INTO `{TableName}`
        ({ColumnString})
        VALUES ({PlaceholderString})
    """

    ExecuteQuery(Query, Values)


# ============================================================
# REPLACE / TRUNCATE / DROP
# ============================================================

def ReplaceTable(
    DataFrame: pl.DataFrame,
    TableName: str,
    BatchSize: int = 5000,
) -> None:
    TruncateTable(TableName)
    WriteTable(DataFrame, TableName, BatchSize)


def TruncateTable(TableName: str) -> None:
    Query = f"TRUNCATE TABLE `{TableName}`"
    ExecuteQuery(Query)


def DropTable(TableName: str) -> None:
    Query = f"DROP TABLE IF EXISTS `{TableName}`"
    ExecuteQuery(Query)


# ============================================================
# UPDATE / DELETE
# ============================================================

def UpdateRows(
    TableName: str,
    Data: dict,
    WhereCondition: str,
    WhereParams: tuple | None = None,
) -> None:
    if not WhereCondition.strip():
        raise ValueError("WhereCondition is required for safety.")

    SetClause = ", ".join(f"`{Col}` = %s" for Col in Data.keys())

    Query = f"""
        UPDATE `{TableName}`
        SET {SetClause}
        WHERE {WhereCondition}
    """

    Params = tuple(Data.values()) + tuple(WhereParams or ())

    ExecuteQuery(Query, Params)


def DeleteRows(
    TableName: str,
    WhereCondition: str,
    Params: tuple | None = None,
) -> None:
    if not WhereCondition.strip():
        raise ValueError("WhereCondition is required for safety.")

    Query = f"""
        DELETE FROM `{TableName}`
        WHERE {WhereCondition}
    """

    ExecuteQuery(Query, Params)


# ============================================================
# TABLE INFO FUNCTIONS
# ============================================================

def ShowTables() -> list[str]:
    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        Cursor.execute("SHOW TABLES")
        return [Row[0] for Row in Cursor.fetchall()]

    finally:
        Cursor.close()
        Conn.close()


def TableExists(TableName: str) -> bool:
    Query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = %s
    """

    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        Cursor.execute(Query, (DB_CONFIG["database"], TableName))
        return Cursor.fetchone()[0] > 0

    finally:
        Cursor.close()
        Conn.close()


def GetColumns(TableName: str) -> list[str]:
    Query = f"SHOW COLUMNS FROM `{TableName}`"

    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        Cursor.execute(Query)
        return [Row[0] for Row in Cursor.fetchall()]

    finally:
        Cursor.close()
        Conn.close()


def CountRows(TableName: str) -> int:
    Query = f"SELECT COUNT(*) FROM `{TableName}`"

    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        Cursor.execute(Query)
        return Cursor.fetchone()[0]

    finally:
        Cursor.close()
        Conn.close()


# ============================================================
# GENERAL QUERY EXECUTION
# ============================================================

def ExecuteQuery(
    Query: str,
    Params: tuple | None = None,
) -> None:
    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        Cursor.execute(Query, Params or ())
        Conn.commit()

    except Exception:
        Conn.rollback()
        raise

    finally:
        Cursor.close()
        Conn.close()


def ExecuteMany(
    Query: str,
    Data: list[tuple],
    BatchSize: int = 5000,
) -> None:
    if not Data:
        return

    Conn = GetConnection()
    Cursor = Conn.cursor()

    try:
        for Start in range(0, len(Data), BatchSize):
            Batch = Data[Start:Start + BatchSize]
            Cursor.executemany(Query, Batch)

        Conn.commit()

    except Exception:
        Conn.rollback()
        raise

    finally:
        Cursor.close()
        Conn.close()


# ============================================================
# CSV IMPORT / EXPORT
# ============================================================

def ImportCsvToTable(
    CsvPath: str,
    TableName: str,
    Replace: bool = False,
    BatchSize: int = 5000,
) -> None:
    DataFrame = pl.read_csv(CsvPath)

    if Replace:
        ReplaceTable(DataFrame, TableName, BatchSize)
    else:
        WriteTable(DataFrame, TableName, BatchSize)


def ExportTableToCsv(
    TableName: str,
    CsvPath: str,
) -> None:
    DataFrame = ReadTable(TableName)
    DataFrame.write_csv(CsvPath)


# ============================================================
# MAIN TEST
# ============================================================

if __name__ == "__main__":
    print("Tables:")
    print(ShowTables())

    print("\nTable Exists:")
    print(TableExists("stock_update"))

    print("\nColumns:")
    print(GetColumns("stock_update"))

    print("\nTotal Rows:")
    print(CountRows("stock_update"))

    print("\nData Preview:")
    df = ReadLimit("stock_update", 5)
    print(df)