from __future__ import annotations

from pathlib import Path

import pytest

from apple_data_analysis.pipeline import run_pipeline, write_pipeline_outputs
from apple_data_analysis.prepare import prepare_sources


@pytest.mark.integration
def test_complete_mixed_source_pipeline(spark, raw_dir, tmp_path):
    prepared_dir = tmp_path / "prepared"
    output_dir = tmp_path / "outputs"

    source_paths = prepare_sources(spark, raw_dir, str(prepared_dir))
    results, quality_report = run_pipeline(spark, **source_paths)
    manifest = write_pipeline_outputs(results, quality_report, str(output_dir))

    assert quality_report == {
        "transaction_rows": 10,
        "customer_rows": 4,
        "product_rows": 4,
        "transaction_null_rows": 0,
        "customer_null_rows": 0,
        "product_null_rows": 0,
        "duplicate_transaction_ids": 0,
        "duplicate_customer_ids": 0,
        "duplicate_product_names": 0,
        "unmatched_customers": 0,
        "unmatched_products": 0,
        "non_positive_prices": 0,
    }
    assert manifest["iphone_then_airpods"]["row_count"] == 2
    assert manifest["only_iphone_airpods"]["row_count"] == 2
    assert manifest["subsequent_purchases"]["row_count"] == 4
    assert manifest["average_purchase_delay"]["row_count"] == 1
    assert manifest["top_products"]["row_count"] == 3

    assert (prepared_dir / "customers_delta" / "_delta_log").is_dir()
    assert (output_dir / "only_iphone_airpods" / "_delta_log").is_dir()
    assert (output_dir / "manifest.json").is_file()
    assert (output_dir / "quality_report.json").is_file()
