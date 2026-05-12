# Data And Code Guide

This guide explains the main data assets and Python modules in the pricing
management system. The Python files also include module-level docstrings and
targeted function docstrings for quick in-editor context.

## Live Application Modules

- `pricing_system/main.py`: FastAPI entrypoint. Mounts routers, serves HTML
  pages, initializes the database, and starts the periodic refresh pipeline.
- `pricing_system/database.py`: Database bootstrap and user store. Creates core
  MySQL tables, manages `Data/users.json`, and builds Amazon sales schemas.
- `pricing_system/auth.py`: Login, JWT authentication, admin/page guards, and
  special-password validation for sensitive changes.
- `pricing_system/admin.py`: Admin Panel APIs for users, permissions, stats, and
  import history.
- `pricing_system/audit.py`: JSON-backed audit log and import history helpers.
- `pricing_system/logs.py`: Admin audit-log listing and action-filter APIs.
- `pricing_system/items.py`: Item Master APIs for listing, filtering, editing,
  importing, exporting, inventory refresh, and missing-SKU error rows.
- `pricing_system/amazon.py`: Amazon Pricing APIs for listing, filtering,
  exporting, Cost Into % overrides, and Amazon pricing refresh.
- `pricing_system/data_pipeline.py`: Inventory and Amazon pricing refresh logic.
  It rebuilds `stock_update`, `stock_items`, and `amazon_pricing_results`.
- `pricing_system/amazon_sales_history.py`: Imports historical Amazon B2B/B2C
  sales CSVs into `amazon_sales_b2b` and `amazon_sales_b2c`.
- `pricing_system/sync_catalog.py`: Manual helper to upsert `CatalogData.csv`
  into `catalog_pricing`.

## Manual Diagnostic Scripts

- `pricing_system/verify_cost_percent.py`: Checks `stock_items.cost_into_percent`
  values in MySQL.
- `pricing_system/test_amazon_columns.py`: Logs into a local API and checks
  Amazon Pricing column metadata.
- `pricing_system/test_amazon_data.py`: Logs into a local API and checks sample
  Amazon Pricing data.
- `pricing_system/test_join.py`: Checks joins between `amazon_pricing_results`
  and `stock_items`.
- `pricing_system/test_server.py`: Basic local-server/login/API smoke test.

## Legacy And Prototype Scripts

- `ProcessFiles/transform_data.py`: Older CSV-based stock and item-master
  transformation prototype.
- `ProcessFiles/SQLDataUpdater.py`: Generic CSV/Excel to MySQL table loader.
- `ProcessFiles/SQLTest.py`: DuckDB/pandas/MySQL proof of concept.
- `ProcessFiles/AmazonPricingModule/AmazonPricing.py`: Production Amazon
  pricing calculation engine used by `data_pipeline.py`.
- `ProcessFiles/AmazonPricingModule/Test.py`, `Test2.py`, `Test3.py`: Earlier
  Amazon pricing/rate-card prototypes kept for reference.
- `ProcessFiles/AmazonPricingModule/Test4.py`: Manual `CatalogData.csv`
  deduplication helper.
- `ProcessFiles/AmazonPricingModule/patch.py`: Historical one-time patch script.

## Data Folder

- `Data/ItemMaster.csv`: Source-style item master export with Master SKU, style,
  category, location, remarks, and type columns.
- `Data/CatalogData.csv`: Catalog/pricing source data such as launch date,
  catalog name, cost, wholesale price, up price, and MRP.
- `Data/StockUpdate*.csv`, `Data/StockCleaned.csv`: Stock source/derived files
  from Uniware/FBA/FBF/SJIT feeds. The live pipeline now pulls stock from the
  configured Google Sheet URL.
- `Data/PricingModuleData.csv`, `Data/FinalOutput.csv`: Derived output snapshots
  from previous data-processing flows.
- `Data/users.json`: Local user accounts, password hashes, roles, page access,
  and column permissions.
- `Data/logs.json`: Audit log entries created by login, import, edit, and admin
  actions.
- `Data/imports.json`: Import-history summaries shown in the Admin Panel.
- `Data/OldPricing.xlsx`: Historical pricing workbook used as reference data.

## Amazon Data Folder

- `Data/AmazonData/AmazonRateCard.xlsx`: Amazon fee/rate-card workbook used by
  the Amazon pricing engine.
- `Data/AmazonData/AmazonPricingModule.xlsx`: Supporting Amazon pricing workbook.
- `Data/AmazonData/Amazon_Pricing_Result.csv` and
  `Amazon_New_Pricing_Result.xlsx`: Generated Amazon pricing outputs.
- `Data/AmazonData/AmazonSales.xlsm`: Amazon sales workbook/macro source.
- `Data/AmazonData/AmazonSalesHistory/<year>/`: Monthly Amazon MTR sales CSVs.
  Files containing `B2B` load into `amazon_sales_b2b`; files containing `B2C`
  load into `amazon_sales_b2c`. CSV column names are preserved with spaces
  replaced by underscores.
