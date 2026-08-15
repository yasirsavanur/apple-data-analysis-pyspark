from __future__ import annotations

from pathlib import Path

import pytest

from apple_data_analysis.spark import create_spark_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    root = Path(tmp_path_factory.mktemp("spark-session"))
    session = create_spark_session(
        app_name="AppleDataAnalysisTests",
        master="local[2]",
        warehouse_dir=str(root / "warehouse"),
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw_dir() -> str:
    return str(Path(__file__).resolve().parents[1] / "data" / "raw")
