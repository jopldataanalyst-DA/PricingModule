import polars as pl
from contextlib import contextmanager
from mysql.connector import pooling


class MySqlDatabase:
    def __init__(
        self,
        DbConfig: dict,
        PoolName: str = "MySqlPool",
        PoolSize: int = 10,
    ):
        self.DbConfig = DbConfig
        self.DatabaseName = DbConfig["database"]

        self.Pool = pooling.MySQLConnectionPool(
            pool_name=PoolName,
            pool_size=PoolSize,
            pool_reset_session=True,
            **DbConfig,
        )

    def GetConnection(self):
        return self.Pool.get_connection()

    @staticmethod
    def QuoteIdentifier(Name: str) -> str:
        return "`" + str(Name).replace("`", "``") + "`"

    @contextmanager
    def Connection(self):
        Conn = self.GetConnection()
        try:
            yield Conn
        finally:
            Conn.close()

    @contextmanager
    def Cursor(self, Dictionary: bool = False, Commit: bool = False):
        Conn = self.GetConnection()
        Cursor = Conn.cursor(dictionary=Dictionary)

        try:
            yield Cursor
            if Commit:
                Conn.commit()

        except Exception:
            if Commit:
                Conn.rollback()
            raise

        finally:
            Cursor.close()
            Conn.close()

    # ============================================================
    # READ
    # ============================================================

    def ReadTable(self, TableName: str) -> pl.DataFrame:
        return self.ExecuteReadQuery(f"SELECT * FROM `{TableName}`")

    def ReadLimit(self, TableName: str, Limit: int = 10) -> pl.DataFrame:
        return self.ExecuteReadQuery(
            f"SELECT * FROM `{TableName}` LIMIT %s",
            (Limit,),
        )

    def ReadWhere(
        self,
        TableName: str,
        WhereCondition: str,
        Params: tuple | None = None,
    ) -> pl.DataFrame:
        return self.ExecuteReadQuery(
            f"SELECT * FROM `{TableName}` WHERE {WhereCondition}",
            Params,
        )

    def ExecuteReadQuery(
        self,
        Query: str,
        Params: tuple | None = None,
    ) -> pl.DataFrame:
        Conn = self.GetConnection()
        Cursor = Conn.cursor(dictionary=True)

        try:
            Cursor.execute(Query, Params or ())
            Data = Cursor.fetchall()
            if not Data:
                return pl.DataFrame()
            
            try:
                # Use infer_schema_length=None to scan all rows for correct data types
                # This prevents ComputeError when mixed types (like strings and numbers) appear later in the dataset
                return pl.from_dicts(Data, infer_schema_length=None)
            except Exception:
                # Fallback: Convert to dict of lists which can sometimes be more robust for Polars inference
                Columns = Data[0].keys()
                DataDict = {col: [row[col] for row in Data] for col in Columns}
                return pl.from_dict(DataDict)

        finally:
            Cursor.close()
            Conn.close()

    def FetchAll(
        self,
        Query: str,
        Params: tuple | None = None,
        Dictionary: bool = True,
    ) -> list:
        with self.Cursor(Dictionary=Dictionary) as Cursor:
            Cursor.execute(Query, Params or ())
            return Cursor.fetchall()

    def FetchOne(
        self,
        Query: str,
        Params: tuple | None = None,
        Dictionary: bool = False,
    ):
        with self.Cursor(Dictionary=Dictionary) as Cursor:
            Cursor.execute(Query, Params or ())
            return Cursor.fetchone()

    def FetchValue(
        self,
        Query: str,
        Params: tuple | None = None,
        Default=None,
    ):
        Row = self.FetchOne(Query, Params, Dictionary=False)
        if Row is None:
            return Default
        return Row[0]

    # ============================================================
    # WRITE
    # ============================================================

    def WriteTable(
        self,
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
        self.ExecuteMany(Query, Rows, BatchSize)

    def InsertRow(self, TableName: str, Data: dict) -> None:
        Columns = list(Data.keys())
        Values = tuple(Data.values())

        ColumnString = ", ".join(f"`{Col}`" for Col in Columns)
        PlaceholderString = ", ".join(["%s"] * len(Columns))

        Query = f"""
            INSERT INTO `{TableName}`
            ({ColumnString})
            VALUES ({PlaceholderString})
        """

        self.ExecuteQuery(Query, Values)

    def UpsertRows(
        self,
        TableName: str,
        Rows: list[dict],
        UpdateColumns: list[str] | None = None,
        BatchSize: int = 5000,
    ) -> int:
        if not Rows:
            return 0

        Columns = list(Rows[0].keys())
        if not Columns:
            return 0

        for Row in Rows:
            MissingColumns = [Column for Column in Columns if Column not in Row]
            if MissingColumns:
                raise ValueError(f"Missing columns for upsert: {MissingColumns}")

        UpdateColumns = UpdateColumns or Columns
        ColumnString = ", ".join(self.QuoteIdentifier(Column) for Column in Columns)
        PlaceholderString = ", ".join(["%s"] * len(Columns))
        UpdateString = ", ".join(
            f"{self.QuoteIdentifier(Column)}=VALUES({self.QuoteIdentifier(Column)})"
            for Column in UpdateColumns
        )

        Query = f"""
            INSERT INTO {self.QuoteIdentifier(TableName)}
            ({ColumnString})
            VALUES ({PlaceholderString})
            ON DUPLICATE KEY UPDATE {UpdateString}
        """

        Data = [tuple(Row.get(Column) for Column in Columns) for Row in Rows]
        return self.ExecuteMany(Query, Data, BatchSize)

    def UpsertTable(
        self,
        DataFrame: pl.DataFrame,
        TableName: str,
        UpdateColumns: list[str] | None = None,
        BatchSize: int = 5000,
    ) -> int:
        if DataFrame.is_empty():
            return 0
        return self.UpsertRows(
            TableName=TableName,
            Rows=DataFrame.to_dicts(),
            UpdateColumns=UpdateColumns,
            BatchSize=BatchSize,
        )

    # ============================================================
    # REPLACE / DELETE
    # ============================================================

    def ReplaceTable(
        self,
        DataFrame: pl.DataFrame,
        TableName: str,
        BatchSize: int = 5000,
    ) -> None:
        self.TruncateTable(TableName)
        self.WriteTable(DataFrame, TableName, BatchSize)

    def DeleteRows(
        self,
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

        self.ExecuteQuery(Query, Params)

    def TruncateTable(self, TableName: str) -> None:
        self.ExecuteQuery(f"TRUNCATE TABLE `{TableName}`")

    def DropTable(self, TableName: str) -> None:
        self.ExecuteQuery(f"DROP TABLE IF EXISTS `{TableName}`")

    # ============================================================
    # UPDATE
    # ============================================================

    def UpdateRows(
        self,
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
        self.ExecuteQuery(Query, Params)

    # ============================================================
    # TABLE INFO
    # ============================================================

    def ShowTables(self) -> list[str]:
        Conn = self.GetConnection()
        Cursor = Conn.cursor()

        try:
            Cursor.execute("SHOW TABLES")
            return [Row[0] for Row in Cursor.fetchall()]

        finally:
            Cursor.close()
            Conn.close()

    def TableExists(self, TableName: str) -> bool:
        Query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        """

        Conn = self.GetConnection()
        Cursor = Conn.cursor()

        try:
            Cursor.execute(Query, (self.DatabaseName, TableName))
            return Cursor.fetchone()[0] > 0

        finally:
            Cursor.close()
            Conn.close()

    def GetColumns(self, TableName: str) -> list[str]:
        Conn = self.GetConnection()
        Cursor = Conn.cursor()

        try:
            Cursor.execute(f"SHOW COLUMNS FROM `{TableName}`")
            return [Row[0] for Row in Cursor.fetchall()]

        finally:
            Cursor.close()
            Conn.close()

    def CountRows(self, TableName: str) -> int:
        Conn = self.GetConnection()
        Cursor = Conn.cursor()

        try:
            Cursor.execute(f"SELECT COUNT(*) FROM `{TableName}`")
            return Cursor.fetchone()[0]

        finally:
            Cursor.close()
            Conn.close()

    # ============================================================
    # GENERAL QUERY
    # ============================================================

    def ExecuteQuery(
        self,
        Query: str,
        Params: tuple | None = None,
    ) -> int:
        Conn = self.GetConnection()
        Cursor = Conn.cursor()

        try:
            Cursor.execute(Query, Params or ())
            Conn.commit()
            return Cursor.rowcount

        except Exception:
            Conn.rollback()
            raise

        finally:
            Cursor.close()
            Conn.close()

    def ExecuteMany(
        self,
        Query: str,
        Data: list[tuple],
        BatchSize: int = 5000,
    ) -> int:
        if not Data:
            return 0

        Conn = self.GetConnection()
        Cursor = Conn.cursor()
        AffectedRows = 0

        try:
            for Start in range(0, len(Data), BatchSize):
                Batch = Data[Start:Start + BatchSize]
                Cursor.executemany(Query, Batch)
                AffectedRows += Cursor.rowcount

            Conn.commit()
            return AffectedRows

        except Exception:
            Conn.rollback()
            raise

        finally:
            Cursor.close()
            Conn.close()

    # ============================================================
    # CSV
    # ============================================================

    def ImportCsvToTable(
        self,
        CsvPath: str,
        TableName: str,
        Replace: bool = False,
        BatchSize: int = 5000,
    ) -> None:
        DataFrame = pl.read_csv(CsvPath, infer_schema_length=10000)

        if Replace:
            self.ReplaceTable(DataFrame, TableName, BatchSize)
        else:
            self.WriteTable(DataFrame, TableName, BatchSize)

    def ExportTableToCsv(
        self,
        TableName: str,
        CsvPath: str,
    ) -> None:
        DataFrame = self.ReadTable(TableName)
        DataFrame.write_csv(CsvPath)

if __name__ == "__main__":
    DB_CONFIG_1 = {
        "host": "localhost",
        "user": "root",
        "password": "123456789",
        "database": "pricing_module",
    }

    DB_CONFIG_2 = {
        "host": "localhost",
        "user": "root",
        "password": "123456789",
        "database": "pricing_module",
    }

    PricingDb = MySqlDatabase(DB_CONFIG_1, PoolName="PricingPool")
    TestDb = MySqlDatabase(DB_CONFIG_2, PoolName="TestPool")

    df = PricingDb.ReadLimit("stock_update", 5)
    print(df)

    df2 = TestDb.ReadLimit("stock_update", 5)
    print(df2)

    print(PricingDb.ShowTables())
    print(TestDb.ShowTables())
