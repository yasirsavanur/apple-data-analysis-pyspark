"""Validate the expected business answers using only Python's standard library."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PRODUCT_NAME_MAP = {
    "iPhone SE": "iPhone",
    "AirPods Pro": "AirPods",
    "MacBook Air": "MacBook",
    "iPad Mini": "iPad",
}


def read_csv(name: str) -> list[dict[str, str]]:
    with (RAW / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    transactions = read_csv("Transaction_Updated.csv")
    customers = read_csv("Customer_Updated.csv")
    products = read_csv("Products_Updated.csv")

    history: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in transactions:
        history[int(row["customer_id"])].append(
            {
                "transaction_id": int(row["transaction_id"]),
                "product_name": row["product_name"],
                "transaction_date": date.fromisoformat(row["transaction_date"]),
            }
        )
    for purchases in history.values():
        purchases.sort(key=lambda row: (row["transaction_date"], row["transaction_id"]))

    immediate_pairs: list[tuple[int, int]] = []
    only_bundle: list[int] = []
    product_counts: dict[str, int] = defaultdict(int)
    for customer_id, purchases in history.items():
        product_set = {str(row["product_name"]) for row in purchases}
        if product_set == {"iPhone", "AirPods"}:
            only_bundle.append(customer_id)
        for row in purchases:
            product_counts[str(row["product_name"])] += 1
        for current, following in zip(purchases, purchases[1:]):
            if current["product_name"] == "iPhone" and following["product_name"] == "AirPods":
                delay = (following["transaction_date"] - current["transaction_date"]).days
                immediate_pairs.append((customer_id, delay))

    standardised_products = {
        PRODUCT_NAME_MAP.get(row["product_name"], row["product_name"])
        for row in products
    }
    transaction_products = {row["product_name"] for row in transactions}
    transaction_customers = {int(row["customer_id"]) for row in transactions}
    customer_ids = {int(row["customer_id"]) for row in customers}

    assert len(transactions) == 10
    assert len(customers) == 4
    assert len(products) == 4
    assert sorted(customer_id for customer_id, _ in immediate_pairs) == [105, 108]
    assert sorted(only_bundle) == [107, 108]
    assert sum(delay for _, delay in immediate_pairs) / len(immediate_pairs) == 3.5
    assert dict(sorted(product_counts.items())) == {
        "AirPods": 4,
        "MacBook": 2,
        "iPhone": 4,
    }
    assert transaction_products <= standardised_products
    assert transaction_customers <= customer_ids

    result = {
        "rows": {"transactions": 10, "customers": 4, "products": 4},
        "iphone_then_airpods_customers": [105, 108],
        "only_iphone_airpods_customers": [107, 108],
        "average_delay_days": 3.5,
        "product_units": dict(sorted(product_counts.items())),
        "unmatched_customers": 0,
        "unmatched_products_after_standardisation": 0,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    print("PASS: reference dataset and expected business results are consistent.")


if __name__ == "__main__":
    main()
