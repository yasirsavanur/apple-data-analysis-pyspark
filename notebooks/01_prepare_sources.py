# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Prepare the mixed-format Apple data sources
# MAGIC Upload the three raw CSV files to the volume path below before running this notebook.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("..")
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from apple_data_analysis.prepare import prepare_sources

# COMMAND ----------

dbutils.widgets.text("raw_dir", "/Volumes/main/default/apple_data/raw")
dbutils.widgets.text("prepared_dir", "/Volumes/main/default/apple_data/prepared")

raw_dir = dbutils.widgets.get("raw_dir")
prepared_dir = dbutils.widgets.get("prepared_dir")

# COMMAND ----------

source_paths = prepare_sources(spark, raw_dir, prepared_dir)
display(source_paths)

# COMMAND ----------

products = spark.read.parquet(source_paths["products_parquet"])
customers = spark.read.format("delta").load(source_paths["customers_delta"])

display(products.orderBy("product_id"))
display(customers.orderBy("customer_id"))
