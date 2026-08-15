# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Run every Apple purchase workflow
# MAGIC Run `01_prepare_sources.py` first.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("..")
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from apple_data_analysis.pipeline import run_pipeline, write_pipeline_outputs

# COMMAND ----------

dbutils.widgets.text("raw_dir", "/Volumes/main/default/apple_data/raw")
dbutils.widgets.text("prepared_dir", "/Volumes/main/default/apple_data/prepared")
dbutils.widgets.text("output_dir", "/Volumes/main/default/apple_data/outputs")

raw_dir = dbutils.widgets.get("raw_dir")
prepared_dir = dbutils.widgets.get("prepared_dir")
output_dir = dbutils.widgets.get("output_dir")

# COMMAND ----------

results, quality_report = run_pipeline(
    spark,
    transactions_csv=f"{raw_dir}/Transaction_Updated.csv",
    customers_delta=f"{prepared_dir}/customers_delta",
    products_parquet=f"{prepared_dir}/products_parquet",
)

display(quality_report)

# COMMAND ----------

for result_name, result_df in results.items():
    print(result_name)
    display(result_df)

# COMMAND ----------

manifest = write_pipeline_outputs(results, quality_report, output_dir)
display(manifest)
