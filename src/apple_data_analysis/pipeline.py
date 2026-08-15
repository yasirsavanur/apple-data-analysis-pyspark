"""End-to-end orchestration and output persistence."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .loaders import LoaderFactory
from .quality import validate_inputs
from .workflows import AppleDataExtractor, FullAppleWorkflow


OUTPUT_FORMATS = {
    "iphone_then_airpods": "parquet",
    "only_iphone_airpods": "delta",
    "subsequent_purchases": "parquet",
    "average_purchase_delay": "parquet",
    "top_products": "parquet",
}


def run_pipeline(
    spark: SparkSession,
    transactions_csv: str,
    customers_delta: str,
    products_parquet: str,
) -> tuple[dict[str, DataFrame], dict[str, int]]:
    inputs = AppleDataExtractor(
        spark,
        transactions_csv=transactions_csv,
        customers_delta=customers_delta,
        products_parquet=products_parquet,
    ).extract()
    quality_report = validate_inputs(
        inputs.transactions, inputs.customers, inputs.products
    )
    results = FullAppleWorkflow().run(inputs)
    return results, quality_report


def write_pipeline_outputs(
    results: dict[str, DataFrame],
    quality_report: dict[str, int],
    output_dir: str,
) -> dict[str, dict[str, str | int]]:
    root = Path(output_dir)
    manifest: dict[str, dict[str, str | int]] = {}
    for name, dataframe in results.items():
        output_format = OUTPUT_FORMATS[name]
        path = str(root / name)
        LoaderFactory.create(output_format).write(dataframe, path)
        manifest[name] = {
            "format": output_format,
            "path": path,
            "row_count": dataframe.count(),
        }

    root.mkdir(parents=True, exist_ok=True)
    (root / "quality_report.json").write_text(
        json.dumps(quality_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest
