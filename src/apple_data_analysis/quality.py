"""Data quality checks that run before business transformations."""

from __future__ import annotations

from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, functions as F


class DataQualityError(ValueError):
    """Raised when input data fails a required quality rule."""


def _null_count(dataframe: DataFrame, columns: list[str]) -> int:
    null_condition = reduce(or_, (F.col(column).isNull() for column in columns))
    return dataframe.filter(null_condition).count()


def _duplicate_key_count(dataframe: DataFrame, key: str) -> int:
    return dataframe.groupBy(key).count().filter(F.col("count") > 1).count()


def validate_inputs(
    transactions: DataFrame,
    customers: DataFrame,
    products: DataFrame,
) -> dict[str, int]:
    """Return an audit report and stop the pipeline if a critical check fails."""

    report = {
        "transaction_rows": transactions.count(),
        "customer_rows": customers.count(),
        "product_rows": products.count(),
        "transaction_null_rows": _null_count(
            transactions,
            ["transaction_id", "customer_id", "product_name", "transaction_date"],
        ),
        "customer_null_rows": _null_count(
            customers,
            ["customer_id", "customer_name", "join_date", "location"],
        ),
        "product_null_rows": _null_count(
            products,
            ["product_id", "product_name", "category", "price"],
        ),
        "duplicate_transaction_ids": _duplicate_key_count(transactions, "transaction_id"),
        "duplicate_customer_ids": _duplicate_key_count(customers, "customer_id"),
        "duplicate_product_names": _duplicate_key_count(products, "product_name"),
        "unmatched_customers": (
            transactions.select("customer_id")
            .distinct()
            .join(customers.select("customer_id").distinct(), "customer_id", "left_anti")
            .count()
        ),
        "unmatched_products": (
            transactions.select("product_name")
            .distinct()
            .join(products.select("product_name").distinct(), "product_name", "left_anti")
            .count()
        ),
        "non_positive_prices": products.filter(F.col("price") <= 0).count(),
    }

    critical_keys = [
        "transaction_null_rows",
        "customer_null_rows",
        "product_null_rows",
        "duplicate_transaction_ids",
        "duplicate_customer_ids",
        "duplicate_product_names",
        "unmatched_customers",
        "unmatched_products",
        "non_positive_prices",
    ]
    failures = {key: report[key] for key in critical_keys if report[key] != 0}
    if failures:
        details = ", ".join(f"{key}={value}" for key, value in failures.items())
        raise DataQualityError(f"Input data failed validation: {details}")

    return report
