"""Read all taxi Parquet files and run basic data-quality checks with PySpark."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXTERNAL_DATA = Path(r"C:\Users\kieup\Desktop\taxi-demand-data\raw")


def find_parquet_files(data_dir: Path) -> list[Path]:
    """Return every Parquet file below *data_dir*, sorted for reproducibility."""
    return sorted(path for path in data_dir.rglob("*.parquet") if path.is_file())


def resolve_data_dir(requested: str | None) -> Path:
    """Use the requested directory, project raw data, or the supplied external source."""
    if requested:
        return Path(requested).expanduser().resolve()

    project_raw = PROJECT_ROOT / "data" / "raw"
    if find_parquet_files(project_raw):
        return project_raw

    return DEFAULT_EXTERNAL_DATA


def build_missing_expressions(df: DataFrame) -> tuple[list[tuple[str, object]], list[tuple[str, object]]]:
    """Build null-count and blank-string-count expressions for every column."""
    null_expressions = []
    blank_expressions = []

    for field in df.schema.fields:
        column = F.col(field.name)
        null_expressions.append(
            (field.name, F.sum(F.when(column.isNull(), 1).otherwise(0)))
        )
        if isinstance(field.dataType, StringType):
            blank_expressions.append(
                (
                    field.name,
                    F.sum(F.when(column.isNotNull() & (F.trim(column) == ""), 1).otherwise(0)),
                )
            )

    return null_expressions, blank_expressions


def read_all_parquet(spark: SparkSession, parquet_files: list[Path]) -> DataFrame:
    """Read files independently, then union them despite year-to-year schema drift."""
    frames: list[DataFrame] = []
    for path in parquet_files:
        frame = spark.read.parquet(str(path))
        # Keep one canonical spelling for the airport fee column.
        if "Airport_fee" in frame.columns and "airport_fee" not in frame.columns:
            frame = frame.withColumnRenamed("Airport_fee", "airport_fee")
        frames.append(frame)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def build_quality_checks(df: DataFrame) -> list[tuple[str, object]]:
    """Return checks that are applicable to the available taxi columns."""
    checks: list[tuple[str, object]] = []
    columns = set(df.columns)

    def add_if_present(column_name: str, label: str, condition: Callable) -> None:
        if column_name in columns:
            checks.append((label, condition(F.col(column_name))))

    add_if_present("passenger_count", "passenger_count <= 0", lambda c: c <= 0)
    add_if_present("trip_distance", "trip_distance < 0", lambda c: c < 0)
    add_if_present("fare_amount", "fare_amount < 0", lambda c: c < 0)
    add_if_present("total_amount", "total_amount < 0", lambda c: c < 0)
    add_if_present("PULocationID", "PULocationID <= 0", lambda c: c <= 0)
    add_if_present("DOLocationID", "DOLocationID <= 0", lambda c: c <= 0)

    if {"tpep_pickup_datetime", "tpep_dropoff_datetime"}.issubset(columns):
        checks.append(
            (
                "dropoff_before_pickup",
                F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
            )
        )

    return checks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("TAXI_DATA_DIR"),
        help="Directory containing Parquet files; searched recursively.",
    )
    parser.add_argument(
        "--report",
        default=str(PROJECT_ROOT / "output" / "data_quality_report.json"),
        help="Path for the JSON data-quality report.",
    )
    args = parser.parse_args()

    data_dir = resolve_data_dir(args.data_dir)
    parquet_files = find_parquet_files(data_dir)
    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found under: {data_dir}")

    print(f"Data directory: {data_dir}")
    print(f"Parquet files: {len(parquet_files)}")
    for path in parquet_files:
        print(f"  - {path.name}")

    spark = (
        SparkSession.builder
        .appName("TaxiDemandDataQuality")
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.files.maxPartitionBytes", "256m")
        # TLC timestamps are local New York wall-clock times.
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = read_all_parquet(spark, parquet_files)

        print("\n=== SCHEMA ===")
        df.printSchema()

        null_expressions, blank_expressions = build_missing_expressions(df)
        checks = build_quality_checks(df)

        # Compute row count, nulls, blanks, and invalid records in one scan.
        aggregate_expressions = [F.count(F.lit(1)).alias("__row_count")]
        aggregate_expressions.extend(
            expression.alias(f"__null__{name}")
            for name, expression in null_expressions
        )
        aggregate_expressions.extend(
            expression.alias(f"__blank__{name}")
            for name, expression in blank_expressions
        )
        aggregate_expressions.extend(
            F.sum(F.when(condition, 1).otherwise(0)).alias(f"__invalid__{label}")
            for label, condition in checks
        )
        aggregate_values = df.agg(*aggregate_expressions).first().asDict()

        row_count = int(aggregate_values["__row_count"] or 0)
        null_counts = {
            name: int(value or 0)
            for name, value in aggregate_values.items()
            if name.startswith("__null__")
        }
        null_counts = {name.removeprefix("__null__"): value for name, value in null_counts.items()}
        blank_counts = {
            name.removeprefix("__blank__"): int(value or 0)
            for name, value in aggregate_values.items()
            if name.startswith("__blank__")
        }
        invalid_counts = {
            name.removeprefix("__invalid__"): int(value or 0)
            for name, value in aggregate_values.items()
            if name.startswith("__invalid__")
        }

        print(f"=== ROW COUNT: {row_count:,} ===")

        print("\n=== NULL COUNTS ===")
        for name, count in null_counts.items():
            print(f"{name}: {count:,} ({count / row_count:.2%})" if row_count else f"{name}: 0")

        if blank_counts:
            print("\n=== BLANK STRING COUNTS ===")
            for name, count in blank_counts.items():
                print(f"{name}: {count:,} ({count / row_count:.2%})" if row_count else f"{name}: 0")

        print("\n=== INVALID DATA COUNTS ===")
        if invalid_counts:
            for label, count in invalid_counts.items():
                print(f"{label}: {count:,}")
        else:
            print("No applicable taxi-specific checks found.")

        report = {
            "data_dir": str(data_dir),
            "parquet_file_count": len(parquet_files),
            "parquet_files": [path.name for path in parquet_files],
            "row_count": row_count,
            "schema": json.loads(df.schema.json()),
            "null_counts": null_counts,
            "blank_string_counts": blank_counts,
            "invalid_counts": invalid_counts,
        }
        report_path = Path(args.report).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nReport written to: {report_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
