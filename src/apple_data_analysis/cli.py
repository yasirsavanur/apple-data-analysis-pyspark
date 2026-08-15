"""Command-line interface for preparing and running the Spark pipeline."""

from __future__ import annotations

import argparse
import json

from .pipeline import run_pipeline, write_pipeline_outputs
from .prepare import prepare_sources
from .spark import create_spark_session


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("prepare", "run", "all"),
        help="Stage to execute.",
    )
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--work-dir", default="build")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--master", default="local[2]")
    return parser


def main() -> None:
    args = _parser().parse_args()
    spark = create_spark_session(master=args.master, warehouse_dir=f"{args.work_dir}/warehouse")
    spark.sparkContext.setLogLevel("WARN")
    try:
        source_paths = {
            "transactions_csv": f"{args.raw_dir}/Transaction_Updated.csv",
            "customers_delta": f"{args.work_dir}/customers_delta",
            "products_parquet": f"{args.work_dir}/products_parquet",
        }
        if args.command in {"prepare", "all"}:
            source_paths = prepare_sources(spark, args.raw_dir, args.work_dir)
            print("Prepared sources:")
            print(json.dumps(source_paths, indent=2))

        if args.command in {"run", "all"}:
            results, quality_report = run_pipeline(spark, **source_paths)
            manifest = write_pipeline_outputs(
                results, quality_report, args.output_dir
            )
            print("Data quality report:")
            print(json.dumps(quality_report, indent=2, sort_keys=True))
            print("Output manifest:")
            print(json.dumps(manifest, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
