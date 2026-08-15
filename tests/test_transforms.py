from __future__ import annotations

from pathlib import Path

from apple_data_analysis.prepare import standardise_products
from apple_data_analysis.readers import ReaderFactory, SourceConfig
from apple_data_analysis.schemas import CUSTOMER_SCHEMA, RAW_PRODUCT_SCHEMA, TRANSACTION_SCHEMA
from apple_data_analysis.transforms import (
    average_iphone_to_airpods_delay,
    iphone_then_airpods,
    only_iphone_and_airpods,
    purchases_after_first,
    top_products_by_revenue,
)


def _read_raw(spark, raw_dir, name, schema):
    return ReaderFactory.create(
        spark,
        SourceConfig("csv", str(Path(raw_dir) / name), schema=schema),
    ).read()


def test_business_transformations_match_reference_answers(spark, raw_dir):
    transactions = _read_raw(
        spark, raw_dir, "Transaction_Updated.csv", TRANSACTION_SCHEMA
    )
    customers = _read_raw(spark, raw_dir, "Customer_Updated.csv", CUSTOMER_SCHEMA)
    products = standardise_products(
        _read_raw(spark, raw_dir, "Products_Updated.csv", RAW_PRODUCT_SCHEMA)
    )

    first_workflow = iphone_then_airpods(transactions, customers)
    first_rows = {
        row.customer_id: row.days_between for row in first_workflow.collect()
    }
    assert first_rows == {105: 3, 108: 4}

    second_ids = {
        row.customer_id
        for row in only_iphone_and_airpods(transactions, customers).collect()
    }
    assert second_ids == {107, 108}

    subsequent = {
        row.customer_id: (row.first_product, row.subsequent_products)
        for row in purchases_after_first(transactions, customers).collect()
    }
    assert subsequent == {
        105: ("iPhone", ["AirPods", "MacBook"]),
        106: ("iPhone", ["MacBook", "AirPods"]),
        107: ("AirPods", ["iPhone"]),
        108: ("iPhone", ["AirPods"]),
    }

    delay = average_iphone_to_airpods_delay(first_workflow).first()
    assert delay.average_days_iphone_to_airpods == 3.5
    assert delay.qualifying_sequences == 2

    top_products = {
        row.product_name: (row.units_sold, row.revenue, row.revenue_rank)
        for row in top_products_by_revenue(transactions, products).collect()
    }
    assert top_products == {
        "MacBook": (2, 2000.0, 1),
        "iPhone": (4, 1800.0, 2),
        "AirPods": (4, 1000.0, 3),
    }
