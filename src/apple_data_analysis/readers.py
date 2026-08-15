"""Factory Pattern readers for CSV, Parquet and Delta sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class SourceConfig:
    source_type: str
    path: str
    schema: StructType | None = None
    options: dict[str, Any] = field(default_factory=dict)


class DataReader(ABC):
    def __init__(self, spark: SparkSession, config: SourceConfig) -> None:
        self.spark = spark
        self.config = config

    @abstractmethod
    def read(self) -> DataFrame:
        """Return the configured source as a Spark DataFrame."""


class CSVReader(DataReader):
    def read(self) -> DataFrame:
        reader = self.spark.read
        if self.config.schema is not None:
            reader = reader.schema(self.config.schema)
        options = {
            "header": True,
            "mode": "FAILFAST",
            "dateFormat": "yyyy-MM-dd",
            **self.config.options,
        }
        return reader.options(**options).csv(self.config.path)


class ParquetReader(DataReader):
    def read(self) -> DataFrame:
        return self.spark.read.options(**self.config.options).parquet(self.config.path)


class DeltaReader(DataReader):
    def read(self) -> DataFrame:
        return self.spark.read.format("delta").options(**self.config.options).load(self.config.path)


class ReaderFactory:
    _READERS = {
        "csv": CSVReader,
        "parquet": ParquetReader,
        "delta": DeltaReader,
    }

    @classmethod
    def create(cls, spark: SparkSession, config: SourceConfig) -> DataReader:
        try:
            reader_class = cls._READERS[config.source_type.lower()]
        except KeyError as exc:
            supported = ", ".join(sorted(cls._READERS))
            raise ValueError(
                f"Unsupported source type '{config.source_type}'. Supported types: {supported}."
            ) from exc
        return reader_class(spark, config)
