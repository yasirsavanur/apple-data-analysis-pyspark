"""Output loaders used by the ETL workflows."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class DataLoader(ABC):
    @abstractmethod
    def write(
        self,
        dataframe: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
    ) -> None:
        """Write a Spark DataFrame to a configured sink."""


class ParquetLoader(DataLoader):
    def write(
        self,
        dataframe: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
    ) -> None:
        writer = dataframe.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)


class DeltaLoader(DataLoader):
    def write(
        self,
        dataframe: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
    ) -> None:
        writer = dataframe.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)


class LoaderFactory:
    _LOADERS = {
        "parquet": ParquetLoader,
        "delta": DeltaLoader,
    }

    @classmethod
    def create(cls, output_type: str) -> DataLoader:
        try:
            return cls._LOADERS[output_type.lower()]()
        except KeyError as exc:
            supported = ", ".join(sorted(cls._LOADERS))
            raise ValueError(
                f"Unsupported output type '{output_type}'. Supported types: {supported}."
            ) from exc
