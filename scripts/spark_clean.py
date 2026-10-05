#!/usr/bin/env python3
"""
DA3408 Assignment 3 - Spark pipeline.

Memory-safe design:
- Discovers all 36 monthly TLC Parquet files.
- Processes one month at a time, but keeps ONE end-to-end benchmark timer.
- Uses the same logical cleansing, zone enrichment, and average-speed logic
  for all months.
- mode=native uses Spark SQL expressions.
- mode=udf uses a Python UDF for average speed.

This is a memory-management strategy, not 36 separate benchmarks.
"""

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType


PROJECT_DIR = Path.home() / "AIOps_Assignment3"
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_OUTPUT = PROJECT_DIR / "output" / "spark_result"
BENCHMARK_DIR = PROJECT_DIR / "benchmark"

REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
]

FINAL_COLUMNS = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date",
    "passenger_count",
    "trip_distance",
    "duration_seconds",
    "average_speed_mph",
    "PULocationID",
    "DOLocationID",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "fare_amount",
    "total_amount",
]


def calculate_average_speed(distance, duration_seconds):
    if distance is None or duration_seconds is None or duration_seconds <= 0:
        return None
    return float(distance) * 3600.0 / float(duration_seconds)


average_speed_udf = F.udf(calculate_average_speed, DoubleType())


def normalize_trip(df):
    """Normalize the three observed TLC schema variants."""
    numeric_double = [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    for col in numeric_double:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("double"))
        else:
            df = df.withColumn(col, F.lit(None).cast("double"))

    for col in ["VendorID", "PULocationID", "DOLocationID"]:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("long"))
        else:
            df = df.withColumn(col, F.lit(None).cast("long"))

    df = (
        df.withColumn("pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
          .withColumn("dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    )

    df = df.withColumn(
        "duration_seconds",
        F.col("dropoff_datetime").cast("long")
        - F.col("pickup_datetime").cast("long"),
    )

    return df.select(
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "duration_seconds",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "total_amount",
    )


def clean_trip(df):
    """Apply the same cleansing logic used by Ray."""
    df = df.dropna(subset=REQUIRED_COLUMNS)
    df = df.filter(F.col("trip_distance") >= 0)
    df = df.filter(F.col("duration_seconds") > 0)
    return df.dropDuplicates()


def load_zones(spark):
    zone_path = str(DATA_DIR / "taxi_zone_lookup.csv")

    zones = (
        spark.read.option("header", True).csv(zone_path)
        .select(
            F.col("LocationID").cast("long").alias("LocationID"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone"),
        )
        .dropDuplicates(["LocationID"])
    )
    return zones


def transform(df, zones, mode):
    """Join the large trip data with the taxi-zone lookup and derive speed."""
    enriched = (
        df.join(
            zones,
            df.PULocationID == zones.LocationID,
            "left",
        )
        .drop("LocationID")
    )

    if mode == "native":
        enriched = enriched.withColumn(
            "average_speed_mph",
            F.col("trip_distance") * F.lit(3600.0)
            / F.col("duration_seconds"),
        )
    else:
        enriched = enriched.withColumn(
            "average_speed_mph",
            average_speed_udf(
                F.col("trip_distance"),
                F.col("duration_seconds"),
            ),
        )

    enriched = enriched.withColumn(
        "pickup_date",
        F.to_date("pickup_datetime").cast("string"),
    )

    return enriched.select(*FINAL_COLUMNS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["native", "udf"],
        default="native",
        help="Average-speed implementation.",
    )
    parser.add_argument(
        "--max-months",
        type=int,
        default=36,
        help="Smoke-test option. Default 36; use 1 only for a non-benchmark smoke test.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory.",
    )
    args = parser.parse_args()

    trip_files = sorted(DATA_DIR.glob("yellow_tripdata_*.parquet"))
    if not trip_files:
        raise FileNotFoundError(f"No yellow_tripdata_*.parquet files found in {DATA_DIR}")

    if args.max_months < 1 or args.max_months > len(trip_files):
        raise ValueError(f"--max-months must be between 1 and {len(trip_files)}")

    trip_files = trip_files[: args.max_months]

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else DEFAULT_OUTPUT.parent / f"{DEFAULT_OUTPUT.name}_{args.mode}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(f"DA3408_Spark_{args.mode}")
        .master("spark://10.0.2.15:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    zones = load_zones(spark)

    start = time.perf_counter()
    month_times = []

    try:
        for index, trip_file in enumerate(trip_files, start=1):
            month = trip_file.stem.replace("yellow_tripdata_", "")
            month_output = output_dir / f"month={month}"

            month_start = time.perf_counter()

            df = spark.read.parquet(str(trip_file))
            df = normalize_trip(df)
            df = clean_trip(df)
            result = transform(df, zones, args.mode)

            # Write is the terminal action. We intentionally do NOT call count()
            # afterward because that would execute another full pass.
            (
                result.write
                .mode("overwrite")
                .parquet(str(month_output))
            )

            elapsed = time.perf_counter() - month_start
            month_times.append({"month": month, "seconds": elapsed})
            print(
                f"[{index:02d}/{len(trip_files)}] "
                f"{month} completed in {elapsed:.2f}s"
            )

    finally:
        total_elapsed = time.perf_counter() - start
        spark.stop()

    metrics = {
        "framework": "spark",
        "mode": args.mode,
        "months_processed": len(trip_files),
        "total_wall_clock_seconds": total_elapsed,
        "month_times": month_times,
        "output_dir": str(output_dir),
        "udf_stage_timing": (
            "Use Spark UI Stages for the actual PythonUDF stage duration; "
            "the total benchmark timer above includes ingestion through export."
            if args.mode == "udf"
            else "not applicable"
        ),
    }

    metrics_path = BENCHMARK_DIR / f"spark_{args.mode}_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2))

    print("\n=== SPARK BENCHMARK COMPLETE ===")
    print(json.dumps(metrics, indent=2))
    print(f"Metrics: {metrics_path}")


if __name__ == "__main__":
    main()
