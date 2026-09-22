"""Prepare a compact cleaned dataset for TV2 from official Yellow Taxi Parquet files.

This helper is intended for CI/reproducibility when the full cleaned dataset is
not already available. It keeps only the columns needed by TV2 so the
2023-2024 EDA/demand job fits comfortably on a small runner.

For the project hand-off, src/03_eda.py and src/04_aggregate_demand.py still
consume the canonical cleaned columns pickup_datetime and zone_id.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


START_TS = "2023-01-01 00:00:00"
END_EXCLUSIVE_TS = "2025-01-01 00:00:00"


def find_parquet_files(data_dir: Path) -> list[Path]:
    return sorted(path for path in data_dir.rglob("*.parquet") if path.is_file())


def read_minimal(spark: SparkSession, files: list[Path]) -> DataFrame:
    frames: list[DataFrame] = []
    for path in files:
        frame = spark.read.parquet(str(path)).select(
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
            F.col("PULocationID").cast("int").alias("zone_id"),
        )
        frames.append(frame)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()
    files = find_parquet_files(data_dir)
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {data_dir}")

    spark = (
        SparkSession.builder.appName("TaxiDemandTV2MinimalClean")
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        source = read_minimal(spark, files)
        pickup = F.col("pickup_datetime")
        dropoff = F.col("dropoff_datetime")
        zone = F.col("zone_id")

        cleaned = (
            source.filter(pickup.isNotNull())
            .filter(
                (pickup >= F.to_timestamp(F.lit(START_TS)))
                & (pickup < F.to_timestamp(F.lit(END_EXCLUSIVE_TS)))
            )
            .filter(zone.between(1, 265))
            .filter(dropoff.isNull() | (dropoff >= pickup))
            .filter(
                dropoff.isNull()
                | ((F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) <= 24 * 60 * 60)
            )
            .dropDuplicates(["pickup_datetime", "dropoff_datetime", "zone_id"])
            .select(
                "pickup_datetime",
                "zone_id",
                F.year("pickup_datetime").alias("pickup_year"),
                F.month("pickup_datetime").alias("pickup_month"),
            )
        )

        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        cleaned.write.mode("overwrite").parquet(str(output))

        stats = cleaned.agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("pickup_datetime").alias("min_pickup"),
            F.max("pickup_datetime").alias("max_pickup"),
            F.countDistinct("zone_id").alias("zones"),
        ).first()

        print(f"Input files: {len(files)}")
        print(f"Rows: {int(stats['rows']):,}")
        print(f"Min pickup: {stats['min_pickup']}")
        print(f"Max pickup: {stats['max_pickup']}")
        print(f"Observed zones: {int(stats['zones'])}")
        print(f"Output: {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
