"""ETL workflows that mirror the tutorial's two named pipelines and extensions."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .readers import ReaderFactory, SourceConfig
from .schemas import TRANSACTION_SCHEMA
from .transforms import (
    average_iphone_to_airpods_delay,
    iphone_then_airpods,
    only_iphone_and_airpods,
    purchases_after_first,
    top_products_by_revenue,
)


@dataclass(frozen=True)
class InputFrames:
    transactions: DataFrame
    customers: DataFrame
    products: DataFrame


class AppleDataExtractor:
    def __init__(
        self,
        spark: SparkSession,
        transactions_csv: str,
        customers_delta: str,
        products_parquet: str,
    ) -> None:
        self.spark = spark
        self.transactions_csv = transactions_csv
        self.customers_delta = customers_delta
        self.products_parquet = products_parquet

    def extract(self) -> InputFrames:
        transactions = ReaderFactory.create(
            self.spark,
            SourceConfig("csv", self.transactions_csv, schema=TRANSACTION_SCHEMA),
        ).read()
        customers = ReaderFactory.create(
            self.spark, SourceConfig("delta", self.customers_delta)
        ).read()
        products = ReaderFactory.create(
            self.spark, SourceConfig("parquet", self.products_parquet)
        ).read()
        return InputFrames(transactions, customers, products)


class FirstWorkflow:
    """AirPods purchased immediately after iPhone."""

    def run(self, inputs: InputFrames) -> DataFrame:
        return iphone_then_airpods(inputs.transactions, inputs.customers)


class SecondWorkflow:
    """Purchase history contains only iPhone and AirPods."""

    def run(self, inputs: InputFrames) -> DataFrame:
        return only_iphone_and_airpods(inputs.transactions, inputs.customers)


class FullAppleWorkflow:
    def run(self, inputs: InputFrames) -> dict[str, DataFrame]:
        first_result = FirstWorkflow().run(inputs)
        return {
            "iphone_then_airpods": first_result,
            "only_iphone_airpods": SecondWorkflow().run(inputs),
            "subsequent_purchases": purchases_after_first(
                inputs.transactions, inputs.customers
            ),
            "average_purchase_delay": average_iphone_to_airpods_delay(first_result),
            "top_products": top_products_by_revenue(
                inputs.transactions, inputs.products
            ),
        }
