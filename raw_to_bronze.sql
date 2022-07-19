-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Databricks <> dbt Hackathon: Retail
-- MAGIC ### Raw to Bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/databricks-datasets/retail-org/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC f = open('/dbfs/databricks-datasets/retail-org/README.md', 'r')
-- MAGIC print(f.read())

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dbx_dbt_retail;
USE dbx_dbt_retail;

-- COMMAND ----------

-- MAGIC %md #### Active Promotions

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS active_promotions;

COPY INTO active_promotions
FROM '/databricks-datasets/retail-org/active_promotions/*.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM active_promotions
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Company Employees

-- COMMAND ----------

-- DBTITLE 0,Company Employees
CREATE TABLE IF NOT EXISTS company_employees;

COPY INTO company_employees
FROM '/databricks-datasets/retail-org/company_employees/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM company_employees
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Loyalty Segments

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS loyalty_segments;

COPY INTO loyalty_segments
FROM '/databricks-datasets/retail-org/loyalty_segments/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM loyalty_segments
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Customers

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers;

COPY INTO customers
FROM '/databricks-datasets/retail-org/customers/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM customers
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Products

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS products;

COPY INTO products
FROM '/databricks-datasets/retail-org/products/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimeter' = ';',
                'header' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

drop table products

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS products;

COPY INTO products
FROM '/databricks-datasets/retail-org/products/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'sep' = ';',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM products
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Promotions

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS promotions;

COPY INTO promotions
FROM '/databricks-datasets/retail-org/promotions/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM promotions
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Purchase Orders

-- COMMAND ----------

-- DBTITLE 0,Purchase Orders
CREATE OR REPLACE TEMP VIEW purchase_orders_temp
USING xml
OPTIONS (path "dbfs:/databricks-datasets/retail-org/purchase_orders", rowTag "purchase_item");

CREATE TABLE IF NOT EXISTS purchase_orders
AS
SELECT * FROM purchase_orders_temp;

-- COMMAND ----------

SELECT * FROM purchase_orders
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Sales Orders

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_orders;

COPY INTO sales_orders
FROM '/databricks-datasets/retail-org/sales_orders/*.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM sales_orders
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Suppliers

-- COMMAND ----------

-- DBTITLE 1,Suppliers
CREATE TABLE IF NOT EXISTS suppliers;

COPY INTO suppliers
FROM '/databricks-datasets/retail-org/suppliers/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM suppliers
LIMIT 1;
