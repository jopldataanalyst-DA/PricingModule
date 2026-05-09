import duckdb
import pandas as pd
from sqlalchemy import create_engine

# MySQL connection string
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456789",
    "database": "pricing_module"
}
mysql_connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"

# Step 1: Create a pandas DataFrame
data = {
    "Name": ["Product A", "Product B", "Product C"],
    "Price": [15.9950, 20.50, 5.9950],
    "Category": ["Electronics", "Clothing", "Groceries"]
}
df = pd.DataFrame(data)

# Step 2: Use DuckDB to update the DataFrame
# Update the price of 'Product A' directly in the DataFrame using DuckDB
updated_df = duckdb.sql("""
    SELECT
        Name,
        Price,
        Category
    FROM df
""").fetchdf()

# Step 3: Push the updated DataFrame to MySQL
engine = create_engine(mysql_connection_string)
updated_df.to_sql(
    name="dummy_data",
    con=engine,
    if_exists="replace",
    index=False
)

print("Data updated and pushed to MySQL!")