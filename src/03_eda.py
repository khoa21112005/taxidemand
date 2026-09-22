"""EDA for NYC Yellow Taxi demand using Spark aggregates and Matplotlib.

The trip-level dataset is read and transformed with Spark DataFrame API.
Only small aggregated result tables are converted to pandas for plotting.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "cleaned" / "taxi_cleaned.parquet"
DEFAULT_CHARTS_DIR = PROJECT_ROOT / "output" / "charts"
DEFAULT_TABLES_DIR = PROJECT_ROOT / "output" / "eda_tables"
START_DATE = "2023-01-01"
END_DATE = "2024-12-31"


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "32"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def resolve_zone_lookup(requested: str | None) -> Path | None:
    if requested:
        path = Path(requested).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Zone lookup not found: {path}")
        return path

    candidates = [
        PROJECT_ROOT / "data" / "lookup" / "taxi_zone_lookup.csv",
        PROJECT_ROOT / "data" / "raw" / "taxi_zone_lookup.csv",
        PROJECT_ROOT / "taxi_zone_lookup.csv",
    ]
    for path in candidates:
        if path.exists():
            return path.resolve()
    return None


def load_zone_lookup(spark: SparkSession, path: Path | None) -> DataFrame | None:
    if path is None:
        return None

    raw = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    lower = {column.lower(): column for column in raw.columns}
    id_col = lower.get("locationid") or lower.get("zone_id")
    zone_col = lower.get("zone") or lower.get("zone_name")
    borough_col = lower.get("borough")

    if not id_col or not zone_col:
        raise ValueError(
            "Zone lookup must contain LocationID/zone_id and Zone/zone_name columns."
        )

    return (
        raw.select(
            F.col(id_col).cast("int").alias("zone_id"),
            F.col(zone_col).cast("string").alias("zone_name"),
            (
                F.col(borough_col).cast("string")
                if borough_col
                else F.lit(None).cast("string")
            ).alias("borough"),
        )
        .filter(F.col("zone_id").isNotNull())
        .dropDuplicates(["zone_id"])
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
        .filter(F.col("zone_id").between(1, 265))
        .filter(
            (F.col("pickup_datetime") >= F.to_timestamp(F.lit("2023-01-01 00:00:00")))
            & (F.col("pickup_datetime") < F.to_timestamp(F.lit("2025-01-01 00:00:00")))
        )
    )


def build_day_hour_grid(spark: SparkSession) -> DataFrame:
    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS pickup_date"
    )
    hours = spark.range(24).select(F.col("id").cast("int").alias("hour_of_day"))
    return (
        dates.crossJoin(hours)
        .withColumn(
            "day_type",
            F.when(F.dayofweek("pickup_date").isin([1, 7]), F.lit("Cuối tuần"))
            .otherwise(F.lit("Ngày thường")),
        )
    )


def plot_hourly(hourly_pdf, output: Path) -> None:
    hourly_pdf = hourly_pdf.sort_values("hour_of_day")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(hourly_pdf["hour_of_day"], hourly_pdf["demand"], marker="o")
    ax.set_title("Nhu cầu taxi theo 24 giờ trong ngày (2023-2024)")
    ax.set_xlabel("Giờ trong ngày")
    ax.set_ylabel("Số chuyến")
    ax.set_xticks(range(24))
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(output, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_weekday_weekend(compare_pdf, output: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for day_type, part in compare_pdf.groupby("day_type"):
        part = part.sort_values("hour_of_day")
        ax.plot(
            part["hour_of_day"],
            part["avg_demand_per_day"],
            marker="o",
            label=day_type,
        )
    ax.set_title("Nhu cầu trung bình theo giờ: Ngày thường và Cuối tuần")
    ax.set_xlabel("Giờ trong ngày")
    ax.set_ylabel("Số chuyến trung bình/ngày")
    ax.set_xticks(range(24))
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_top_zones(top_pdf, output: Path) -> None:
    plot_pdf = top_pdf.sort_values("demand", ascending=True).copy()
    fig, ax = plt.subplots(figsize=(11, 7))
    ax.barh(plot_pdf["zone_label"], plot_pdf["demand"])
    ax.set_title(f"Top {len(plot_pdf)} khu vực đón khách có nhiều chuyến nhất")
    ax.set_xlabel("Số chuyến")
    ax.set_ylabel("Khu vực đón khách")
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--zone-lookup", default=None)
    parser.add_argument("--charts-dir", default=str(DEFAULT_CHARTS_DIR))
    parser.add_argument("--tables-dir", default=str(DEFAULT_TABLES_DIR))
    parser.add_argument("--top-n", type=int, default=15)
    args = parser.parse_args()

    charts_dir = Path(args.charts_dir).expanduser().resolve()
    tables_dir = Path(args.tables_dir).expanduser().resolve()
    charts_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark("TaxiDemandEDA")
    spark.sparkContext.setLogLevel("WARN")

    try:
        trips = prepare_trips(spark.read.parquet(str(Path(args.input).expanduser().resolve()))).cache()
        trip_count = trips.count()
        if trip_count == 0:
            raise RuntimeError("No valid 2023-2024 trips found.")

        hourly = (
            trips.withColumn("hour_of_day", F.hour("pickup_datetime"))
            .groupBy("hour_of_day")
            .agg(F.count(F.lit(1)).alias("demand"))
            .orderBy("hour_of_day")
        )

        observed_daily_hour = (
            trips.withColumn("pickup_date", F.to_date("pickup_datetime"))
            .withColumn("hour_of_day", F.hour("pickup_datetime"))
            .groupBy("pickup_date", "hour_of_day")
            .agg(F.count(F.lit(1)).alias("demand"))
        )

        day_hour_grid = build_day_hour_grid(spark)
        weekday_weekend = (
            day_hour_grid.join(
                observed_daily_hour,
                on=["pickup_date", "hour_of_day"],
                how="left",
            )
            .fillna({"demand": 0})
            .groupBy("day_type", "hour_of_day")
            .agg(F.avg("demand").alias("avg_demand_per_day"))
            .orderBy("day_type", "hour_of_day")
        )

        top_zones = (
            trips.groupBy("zone_id")
            .agg(F.count(F.lit(1)).alias("demand"))
            .orderBy(F.desc("demand"))
            .limit(args.top_n)
        )

        lookup_path = resolve_zone_lookup(args.zone_lookup)
        lookup = load_zone_lookup(spark, lookup_path)
        if lookup is not None:
            top_zones = (
                top_zones.join(lookup, "zone_id", "left")
                .withColumn(
                    "zone_label",
                    F.when(
                        F.col("zone_name").isNotNull() & F.col("borough").isNotNull(),
                        F.concat_ws(" - ", F.col("zone_name"), F.col("borough")),
                    )
                    .when(F.col("zone_name").isNotNull(), F.col("zone_name"))
                    .otherwise(F.concat(F.lit("Zone "), F.col("zone_id"))),
                )
                .orderBy(F.desc("demand"))
            )
        else:
            top_zones = top_zones.withColumn(
                "zone_label", F.concat(F.lit("Zone "), F.col("zone_id"))
            )

        hourly_pdf = hourly.toPandas()
        compare_pdf = weekday_weekend.toPandas()
        top_pdf = top_zones.toPandas()

        hourly_pdf.to_csv(tables_dir / "hourly_demand_24h.csv", index=False)
        compare_pdf.to_csv(tables_dir / "weekday_vs_weekend.csv", index=False)
        top_pdf.to_csv(tables_dir / "top_zones.csv", index=False)

        plot_hourly(hourly_pdf, charts_dir / "01_hourly_demand_24h.png")
        plot_weekday_weekend(compare_pdf, charts_dir / "02_weekday_vs_weekend.png")
        plot_top_zones(top_pdf, charts_dir / "03_top_zones.png")

        print("========== TV2 EDA REPORT ==========")
        print(f"Trips: {trip_count:,}")
        print(f"Zone lookup: {lookup_path if lookup_path else 'not found'}")
        print(f"Charts: {charts_dir}")
        print(f"Tables: {tables_dir}")
        print("Generated:")
        print(" - 01_hourly_demand_24h.png")
        print(" - 02_weekday_vs_weekend.png")
        print(" - 03_top_zones.png")
        print("====================================")

        trips.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
