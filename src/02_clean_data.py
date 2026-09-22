"""Clean NYC Yellow Taxi records for the zone-hour demand pipeline.

The script keeps optional business columns with null values. For demand
forecasting, pickup time and pickup zone are the required fields; fare,
passenger and surcharge columns are not required to count a pickup. Spark
writes Parquet outputs as directories (despite the ``.parquet`` suffix).

Examples
--------
Run on the full raw directory and export the January 2023 hand-off sample::

    python src/02_clean_data.py --data-dir C:\\data\\taxi-demand-data\\raw

Run a small sample without writing the full dataset::

    python src/02_clean_data.py --data-dir C:\\data\\raw\\2023\\01 \\
        --output data/cleaned/sample_cleaned.parquet --skip-sample
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXTERNAL_DATA = Path(r"C:\Users\kieup\Desktop\taxi-demand-data\raw")
START_DATE = "2023-01-01 00:00:00"
END_DATE = "2025-01-01 00:00:00"
DEFAULT_MAX_ZONE_ID = 265


def find_parquet_files(data_dir: Path) -> list[Path]:
    """Return all Parquet files recursively in deterministic order."""
    if data_dir.is_file() and data_dir.suffix.lower() == ".parquet":
        return [data_dir]
    return sorted(path for path in data_dir.rglob("*.parquet") if path.is_file())


def resolve_data_dir(requested: str | None) -> Path:
    """Resolve CLI input, environment input, project raw data, or local default."""
    if requested:
        return Path(requested).expanduser().resolve()

    project_raw = PROJECT_ROOT / "data" / "raw"
    if find_parquet_files(project_raw):
        return project_raw

    return DEFAULT_EXTERNAL_DATA


def read_all_parquet(spark: SparkSession, parquet_files: list[Path]) -> DataFrame:
    """Read each file separately so minor year-to-year schema drift is safe."""
    frames: list[DataFrame] = []
    for path in parquet_files:
        frame = spark.read.parquet(str(path))
        if "Airport_fee" in frame.columns and "airport_fee" not in frame.columns:
            frame = frame.withColumnRenamed("Airport_fee", "airport_fee")
        frames.append(frame)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def add_canonical_columns(df: DataFrame) -> DataFrame:
    """Add stable downstream names while retaining the original taxi columns."""
    required = {"tpep_pickup_datetime", "PULocationID"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Required columns are missing: {', '.join(missing)}")

    return (
        df.withColumn("pickup_datetime", F.col("tpep_pickup_datetime"))
        .withColumn("zone_id", F.col("PULocationID").cast("long"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
    )


def build_invalid_reason(df: DataFrame, max_zone_id: int, require_dropoff: bool) -> DataFrame:
    """Attach one deterministic reason for every row rejected by the contract."""
    pickup = F.col("pickup_datetime")
    zone = F.col("zone_id")
    dropoff = F.col("tpep_dropoff_datetime") if "tpep_dropoff_datetime" in df.columns else None

    reason = (
        F.when(pickup.isNull(), F.lit("pickup_datetime_null"))
        .when(
            (pickup < F.to_timestamp(F.lit(START_DATE)))
            | (pickup >= F.to_timestamp(F.lit(END_DATE))),
            F.lit("pickup_outside_2023_2024"),
        )
        .when(zone.isNull(), F.lit("zone_id_null"))
        .when((zone < 1) | (zone > max_zone_id), F.lit("zone_id_out_of_range"))
    )

    if require_dropoff and dropoff is not None:
        reason = reason.when(dropoff.isNull(), F.lit("dropoff_datetime_null"))

    if dropoff is not None:
        reason = reason.when(
            dropoff.isNotNull() & (dropoff < pickup),
            F.lit("dropoff_before_pickup"),
        ).when(
            dropoff.isNotNull()
            & ((F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) > 24 * 60 * 60),
            F.lit("trip_duration_over_24h"),
        )

    return df.withColumn("_invalid_reason", reason.otherwise(F.lit(None).cast("string")))


def build_spark(app_name: str) -> SparkSession:
    """Build a local or cluster Spark session from environment variables."""
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "32"))
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .config("spark.sql.session.timeZone", "America/New_York")
        # Algorithm v2 avoids a Windows-only native permission probe during commit.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.hadoop.native.lib", "false")
        .getOrCreate()
    )


def write_parquet(df: DataFrame, path: Path) -> None:
    """Overwrite a Spark Parquet directory after creating its parent."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(path))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=os.environ.get("TAXI_DATA_DIR"), help="Raw Parquet directory.")
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data" / "cleaned" / "taxi_cleaned.parquet"),
        help="Full cleaned Parquet output directory.",
    )
    parser.add_argument(
        "--sample-output",
        default=str(PROJECT_ROOT / "data" / "cleaned" / "sample_2023_01.parquet"),
        help="January 2023 hand-off sample output; use --skip-sample to disable.",
    )
    parser.add_argument(
        "--report",
        default=str(PROJECT_ROOT / "output" / "cleaning_report.json"),
        help="JSON report path.",
    )
    parser.add_argument("--max-zone-id", type=int, default=DEFAULT_MAX_ZONE_ID, help="Largest valid TLC zone id.")
    parser.add_argument(
        "--require-dropoff",
        action="store_true",
        help="Reject rows with a null dropoff time; default keeps them for demand counting.",
    )
    parser.add_argument("--skip-sample", action="store_true", help="Do not write the January 2023 sample.")
    args = parser.parse_args()

    started = time.perf_counter()
    data_dir = resolve_data_dir(args.data_dir)
    parquet_files = find_parquet_files(data_dir)
    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found under: {data_dir}")

    print(f"Input directory: {data_dir}")
    print(f"Input files: {len(parquet_files)}")

    spark = build_spark("TaxiDemandCleaning")
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = add_canonical_columns(read_all_parquet(spark, parquet_files))
        flagged = build_invalid_reason(source, args.max_zone_id, args.require_dropoff).cache()

        input_count = flagged.count()
        reason_rows = flagged.groupBy("_invalid_reason").count().collect()
        invalid_counts = {
            row["_invalid_reason"]: int(row["count"])
            for row in reason_rows
            if row["_invalid_reason"] is not None
        }

        cleaned = (
            flagged.filter(F.col("_invalid_reason").isNull())
            .drop("_invalid_reason")
            .dropDuplicates()
            .cache()
        )
        valid_before_dedup = input_count - sum(invalid_counts.values())
        valid_count = cleaned.count()

        write_parquet(cleaned, Path(args.output).expanduser().resolve())

        sample_path = None
        sample_count = None
        if not args.skip_sample:
            sample = cleaned.filter(
                (F.col("pickup_year") == 2023) & (F.col("pickup_month") == 1)
            )
            sample_count = sample.count()
            sample_path = Path(args.sample_output).expanduser().resolve()
            write_parquet(sample, sample_path)

        report = {
            "input_directory": str(data_dir),
            "input_file_count": len(parquet_files),
            "input_row_count": input_count,
            "valid_before_dedup": valid_before_dedup,
            "output_row_count": valid_count,
            "duplicate_rows_removed": valid_before_dedup - valid_count,
            "invalid_counts": invalid_counts,
            "start_date_inclusive": START_DATE,
            "end_date_exclusive": END_DATE,
            "max_zone_id": args.max_zone_id,
            "require_dropoff": args.require_dropoff,
            "output_path": str(Path(args.output).expanduser().resolve()),
            "sample_path": str(sample_path) if sample_path else None,
            "sample_row_count": sample_count,
            "elapsed_seconds": round(time.perf_counter() - started, 2),
        }
        report_path = Path(args.report).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"Input rows: {input_count:,}")
        print(f"Invalid rows: {sum(invalid_counts.values()):,}")
        print(f"Clean rows: {valid_count:,}")
        print(f"Cleaned output: {args.output}")
        if sample_path:
            print(f"January 2023 sample: {sample_path} ({sample_count:,} rows)")
        print(f"Report: {report_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
