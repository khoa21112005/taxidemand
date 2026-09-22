"""Build a complete zone_id x pickup_hour taxi-demand grid for 2023-2024."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "cleaned" / "taxi_cleaned.parquet"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "demand_hourly.parquet"
DEFAULT_REPORT = PROJECT_ROOT / "output" / "reports" / "demand_hourly_summary.json"

START_TS = "2023-01-01 00:00:00"
END_TS = "2024-12-31 23:00:00"
END_EXCLUSIVE_TS = "2025-01-01 00:00:00"
MIN_ZONE_ID = 1
MAX_ZONE_ID = 265
EXPECTED_HOURS = 17_544


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def prepare_trips(df: DataFrame) -> DataFrame:
    required = {"pickup_datetime", "zone_id"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    return (
        df.select(
            F.col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            F.col("zone_id").cast("int").alias("zone_id"),
        )
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("zone_id").between(MIN_ZONE_ID, MAX_ZONE_ID))
        .filter(
            (F.col("pickup_datetime") >= F.to_timestamp(F.lit(START_TS)))
            & (F.col("pickup_datetime") < F.to_timestamp(F.lit(END_EXCLUSIVE_TS)))
        )
    )


def build_hour_dimension(spark: SparkSession) -> DataFrame:
    dates = spark.sql(
        "SELECT explode(sequence(to_date('2023-01-01'), to_date('2024-12-31'), interval 1 day)) AS pickup_date"
    )
    hours = spark.range(24).select(F.col("id").cast("int").alias("hour_of_day"))
    return (
        dates.crossJoin(hours)
        .select(
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format("pickup_date", "yyyy-MM-dd"),
                    F.format_string("%02d:00:00", F.col("hour_of_day")),
                )
            ).alias("pickup_hour")
        )
    )


def build_zone_dimension(spark: SparkSession) -> DataFrame:
    return spark.range(MIN_ZONE_ID, MAX_ZONE_ID + 1).select(
        F.col("id").cast("int").alias("zone_id")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()

    spark = build_spark("TaxiDemandAggregate")
    spark.sparkContext.setLogLevel("WARN")

    try:
        trips = prepare_trips(spark.read.parquet(str(input_path))).cache()
        valid_trip_count = trips.count()
        if valid_trip_count == 0:
            raise RuntimeError("No valid 2023-2024 trips found.")

        observed = (
            trips.withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
            .groupBy("zone_id", "pickup_hour")
            .agg(F.count(F.lit(1)).cast("long").alias("demand"))
        )

        zones = build_zone_dimension(spark).cache()
        hours = build_hour_dimension(spark).cache()

        zone_count = zones.count()
        hour_count = hours.count()
        expected_rows = zone_count * hour_count

        if zone_count != 265:
            raise RuntimeError(f"Expected 265 zones, got {zone_count}.")
        if hour_count != EXPECTED_HOURS:
            raise RuntimeError(f"Expected {EXPECTED_HOURS:,} hours, got {hour_count:,}.")

        full_grid = zones.crossJoin(hours)

        demand_hourly = (
            full_grid.join(observed, ["zone_id", "pickup_hour"], "left")
            .fillna({"demand": 0})
            .withColumn("demand", F.col("demand").cast("long"))
            .select("zone_id", "pickup_hour", "demand")
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        (
            demand_hourly.repartition("zone_id")
            .sortWithinPartitions("zone_id", "pickup_hour")
            .write.mode("overwrite")
            .parquet(str(output_path))
        )

        written = spark.read.parquet(str(output_path)).cache()
        stats = written.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.min("pickup_hour").alias("min_pickup_hour"),
            F.max("pickup_hour").alias("max_pickup_hour"),
            F.countDistinct("zone_id").alias("zone_count"),
            F.countDistinct("pickup_hour").alias("hour_count"),
            F.sum(F.when(F.col("demand") == 0, 1).otherwise(0)).alias("zero_demand_rows"),
            F.sum(F.when(F.col("demand").isNull(), 1).otherwise(0)).alias("null_demand_rows"),
        ).first()

        duplicate_pairs = (
            written.groupBy("zone_id", "pickup_hour")
            .count()
            .filter(F.col("count") != 1)
            .count()
        )

        missing_pairs = (
            full_grid.join(
                written.select("zone_id", "pickup_hour"),
                ["zone_id", "pickup_hour"],
                "left_anti",
            )
            .count()
        )

        row_count = int(stats["row_count"])
        min_hour = stats["min_pickup_hour"]
        max_hour = stats["max_pickup_hour"]
        final_zone_count = int(stats["zone_count"])
        final_hour_count = int(stats["hour_count"])

        passed = (
            row_count == expected_rows
            and str(min_hour) == START_TS
            and str(max_hour) == END_TS
            and final_zone_count == 265
            and final_hour_count == EXPECTED_HOURS
            and duplicate_pairs == 0
            and missing_pairs == 0
            and int(stats["null_demand_rows"] or 0) == 0
        )

        report = {
            "input": str(input_path),
            "output": str(output_path),
            "valid_trip_count": valid_trip_count,
            "row_count": row_count,
            "expected_row_count": expected_rows,
            "min_pickup_hour": str(min_hour),
            "max_pickup_hour": str(max_hour),
            "zone_count": final_zone_count,
            "hour_count": final_hour_count,
            "zero_demand_rows": int(stats["zero_demand_rows"] or 0),
            "null_demand_rows": int(stats["null_demand_rows"] or 0),
            "duplicate_pairs": duplicate_pairs,
            "missing_pairs": missing_pairs,
            "completeness_check": "PASS" if passed else "FAIL",
        }

        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print("========== TV2 DEMAND GRID REPORT ==========")
        for key, value in report.items():
            print(f"{key}: {value}")
        print("============================================")

        if not passed:
            raise RuntimeError("Demand grid validation failed.")

        written.unpersist()
        zones.unpersist()
        hours.unpersist()
        trips.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
