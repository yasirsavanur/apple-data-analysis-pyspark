"""Prepare the public CSV files as the mixed-format tutorial sources."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from .loaders import LoaderFactory
from .readers import ReaderFactory, SourceConfig
from .schemas import CUSTOMER_SCHEMA, RAW_PRODUCT_SCHEMA


PRODUCT_NAME_STANDARDISATION = {
    "iPhone SE": "iPhone",
    "AirPods Pro": "AirPods",
    "MacBook Air": "MacBook",
    "iPad Mini": "iPad",
}


def standardise_products(products: DataFrame) -> DataFrame:
    """Align the raw product master with the transaction vocabulary.

    ``source_product_name`` preserves the original public dataset value.
    """

    mapping_items = [
        item
        for pair in PRODUCT_NAME_STANDARDISATION.items()
        for item in (F.lit(pair[0]), F.lit(pair[1]))
    ]
    name_map = F.create_map(*mapping_items)
    return (
        products.withColumnRenamed("product_name", "source_product_name")
        .withColumn(
            "product_name",
            F.coalesce(
                F.element_at(name_map, F.col("source_product_name")),
                F.col("source_product_name"),
            ),
        )
        .select(
            "product_id",
            "product_name",
            "source_product_name",
            "category",
            "price",
        )
    )


def prepare_sources(spark: SparkSession, raw_dir: str, prepared_dir: str) -> dict[str, str]:
    """Convert customers to Delta and standardised products to Parquet."""

    raw = Path(raw_dir)
    prepared = Path(prepared_dir)
    customer_source = str(raw / "Customer_Updated.csv")
    product_source = str(raw / "Products_Updated.csv")
    transaction_source = str(raw / "Transaction_Updated.csv")
    customer_delta = str(prepared / "customers_delta")
    product_parquet = str(prepared / "products_parquet")

    customers = ReaderFactory.create(
        spark,
        SourceConfig("csv", customer_source, schema=CUSTOMER_SCHEMA),
    ).read()
    raw_products = ReaderFactory.create(
        spark,
        SourceConfig("csv", product_source, schema=RAW_PRODUCT_SCHEMA),
    ).read()
    products = standardise_products(raw_products)

    LoaderFactory.create("delta").write(customers, customer_delta)
    LoaderFactory.create("parquet").write(products, product_parquet)

    return {
        "transactions_csv": transaction_source,
        "customers_delta": customer_delta,
        "products_parquet": product_parquet,
    }
