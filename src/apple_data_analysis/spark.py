"""Spark session creation for local execution."""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "ApplePurchaseDataAnalysis",
    master: str = "local[2]",
    warehouse_dir: str | None = None,
) -> SparkSession:
    """Create a local Spark session with Delta Lake enabled.

    Databricks callers should use the existing ``spark`` session instead.
    """

    warehouse = Path(warehouse_dir or "spark-warehouse").resolve()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
