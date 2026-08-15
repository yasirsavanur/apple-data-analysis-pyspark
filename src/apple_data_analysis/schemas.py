"""Explicit schemas for every input used by the pipeline."""

from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("transaction_date", DateType(), nullable=False),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("customer_name", StringType(), nullable=False),
        StructField("join_date", DateType(), nullable=False),
        StructField("location", StringType(), nullable=False),
    ]
)

RAW_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
    ]
)
