# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Inspect Spark optimisation choices
# MAGIC This notebook demonstrates the optimisation concepts discussed in the tutorial.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("raw_dir", "/Volumes/main/default/apple_data/raw")
dbutils.widgets.text("prepared_dir", "/Volumes/main/default/apple_data/prepared")

raw_dir = dbutils.widgets.get("raw_dir")
prepared_dir = dbutils.widgets.get("prepared_dir")

transactions = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_dir}/Transaction_Updated.csv")
)
products = spark.read.parquet(f"{prepared_dir}/products_parquet")

# COMMAND ----------

# Repartition causes a shuffle and can increase parallelism before a wide operation.
repartitioned = transactions.repartition(4, "customer_id")
print("Partitions after repartition:", repartitioned.rdd.getNumPartitions())

# Coalesce normally reduces partitions without a full shuffle.
coalesced = repartitioned.coalesce(2)
print("Partitions after coalesce:", coalesced.rdd.getNumPartitions())

# COMMAND ----------

# Products is a tiny dimension, so a broadcast join avoids shuffling the larger side.
broadcast_join = transactions.join(F.broadcast(products), "product_name", "inner")
broadcast_join.explain("formatted")
display(broadcast_join)

# COMMAND ----------

# The filter is applied directly to the Parquet scan when Spark can push it down.
smartphone_products = spark.read.parquet(
    f"{prepared_dir}/products_parquet"
).filter(F.col("category") == "Smartphone")
smartphone_products.explain("formatted")
display(smartphone_products)
