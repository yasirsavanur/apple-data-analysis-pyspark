from __future__ import annotations

from pathlib import Path

import pytest

from apple_data_analysis.readers import ReaderFactory, SourceConfig
from apple_data_analysis.schemas import TRANSACTION_SCHEMA


def test_csv_reader_uses_explicit_schema(spark, raw_dir):
    dataframe = ReaderFactory.create(
        spark,
        SourceConfig(
            "csv",
            str(Path(raw_dir) / "Transaction_Updated.csv"),
            schema=TRANSACTION_SCHEMA,
        ),
    ).read()

    assert dataframe.count() == 10
    assert dataframe.schema.simpleString() == TRANSACTION_SCHEMA.simpleString()


def test_reader_factory_rejects_unknown_source(spark):
    with pytest.raises(ValueError, match="Unsupported source type"):
        ReaderFactory.create(spark, SourceConfig("excel", "unused.xlsx"))
