#!/usr/bin/env python3
"""
DA3408 Assignment 3 - Ray pipeline (parity-fixed, memory-safe, Ray 2.20).

Goal: produce output IDENTICAL (same rows, same values, same dtypes) to
spark_clean.py, while staying inside ~8 GB RAM.

Parity fixes vs the previous version
------------------------------------
1. Duplicate removal is now done per MONTH (like Spark's dropDuplicates()),
   not per Ray batch. Per-batch dedup missed duplicates spread across batches.
2. duration_seconds is computed from timestamps floored to whole seconds and
   stored as integer (Spark: cast(timestamp as long) - cast(timestamp as long)).
   Before, Ray kept microseconds as a float.
3. Integer columns (VendorID, PULocationID, DOLocationID, duration_seconds)
   are kept as nullable Int64 -> written as int64 (Spark: long), not float.
4. taxi_zone_lookup.csv is read so that strings like "N/A" stay strings
   (pandas turns them into NaN by default; Spark keeps them). Only the empty
   string becomes null, same as Spark's CSV reader.
5. Zone string columns are explicitly typed as string so the schema is stable
   across blocks.
6. average_speed_mph uses the exact same float expression as Spark:
   (trip_distance * 3600.0) / duration_seconds.

Memory design
-------------
- Only the needed parquet columns are read.
- Stage 1 (distributed): read -> normalize -> null/range cleansing.
- Month-level exact dedup on the driver (one cleaned month is only a few
  hundred MB), then re-split into blocks.
- Stage 2 (distributed): zone join -> speed -> pickup_date -> Parquet.
- Object-store usage and block sizes are capped through Ray Data's
  DataContext. No global all-to-all shuffle is used.
"""

import argparse
import gc
import json
import shutil
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import ray
import ray.data as rd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
PROJECT_DIR = Path.home() / "AIOps_Assignment3"
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_OUTPUT = PROJECT_DIR / "output" / "ray_result"
BENCHMARK_DIR = PROJECT_DIR / "benchmark"

# ------------------------------------------------------------
# Tuning knobs (lower these if you still see memory pressure)
# ------------------------------------------------------------
MAP_BATCH_SIZE = 20_000                 # rows per pandas batch
READ_BLOCKS = 16                        # input blocks per monthly file
OUTPUT_CHUNK_ROWS = 250_000             # rows per block after dedup
OBJECT_STORE_LIMIT = 2 * 1024 ** 3      # Ray Data streaming budget (2 GiB)
TARGET_MAX_BLOCK_SIZE = 64 * 1024 ** 2  # 64 MiB blocks

REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
]

# Columns after normalization (identical to Spark's normalize_trip output)
NORMALIZED_COLUMNS = [
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

# The saved Spark outputs were produced with the VM's default timezone
# (Asia/Kolkata, +05:30). Spark treated the naive TLC timestamps as local time
# and stored the UTC instant, i.e. every timestamp is 5h30m earlier than the raw
# value. To get identical output without rerunning Spark, Ray applies the same
# conversion to pickup_datetime / dropoff_datetime (pickup_date and duration are
# unaffected, exactly as in Spark).
SPARK_LOCAL_TZ = "Asia/Kolkata"


def _to_spark_utc(s: pd.Series) -> pd.Series:
    return s.dt.tz_localize(SPARK_LOCAL_TZ).dt.tz_convert("UTC").dt.tz_localize(None)


INT_COLUMNS = ["VendorID", "PULocationID", "DOLocationID", "duration_seconds"]
SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]


# ------------------------------------------------------------
# Python UDF (row-wise, same maths as Spark's UDF)
# ------------------------------------------------------------
def calculate_average_speed(distance, duration_seconds):
    if (
        distance is None
        or duration_seconds is None
        or pd.isna(distance)
        or pd.isna(duration_seconds)
        or duration_seconds <= 0
    ):
        return None
    return float(distance) * 3600.0 / float(duration_seconds)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _numeric(batch, col, kind):
    """Cast a column like Spark's .cast('double'/'long'); missing -> null."""
    if col in batch.columns:
        s = pd.to_numeric(batch[col], errors="coerce")
    else:
        s = pd.Series(pd.NA, index=batch.index, dtype="Float64")
    if kind == "int":
        return s.astype("Int64")
    return s.astype("float64")


def _force_int_types(df):
    for c in INT_COLUMNS:
        if c in df.columns:
            df[c] = df[c].astype("Int64")
    return df


# ------------------------------------------------------------
# Stage 1: normalize + cleanse (row-wise rules, safe per batch)
# ------------------------------------------------------------
def normalize_and_clean_batch(batch: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=batch.index)

    out["VendorID"] = _numeric(batch, "VendorID", "int")

    pickup = pd.to_datetime(batch["tpep_pickup_datetime"], errors="coerce")
    dropoff = pd.to_datetime(batch["tpep_dropoff_datetime"], errors="coerce")
    out["pickup_datetime"] = pickup
    out["dropoff_datetime"] = dropoff

    out["passenger_count"] = _numeric(batch, "passenger_count", "double")
    out["trip_distance"] = _numeric(batch, "trip_distance", "double")

    # Whole-second duration, exactly like Spark's cast("long") difference.
    out["duration_seconds"] = (
        (dropoff.dt.floor("s") - pickup.dt.floor("s"))
        .dt.total_seconds()
        .astype("Int64")
    )

    out["PULocationID"] = _numeric(batch, "PULocationID", "int")
    out["DOLocationID"] = _numeric(batch, "DOLocationID", "int")
    out["fare_amount"] = _numeric(batch, "fare_amount", "double")
    out["total_amount"] = _numeric(batch, "total_amount", "double")

    out = out[NORMALIZED_COLUMNS]

    # Cleansing (same order/logic as Spark's clean_trip, minus dedup)
    out = out.dropna(subset=REQUIRED_COLUMNS)
    out = out[(out["trip_distance"] >= 0).to_numpy()]
    out = out[(out["duration_seconds"] > 0).fillna(False).to_numpy(dtype=bool)]

    return _force_int_types(out.reset_index(drop=True))


# ------------------------------------------------------------
# Stage 2: zone join + derived features
# ------------------------------------------------------------
def enrich_with_zones(batch: pd.DataFrame, zones: pd.DataFrame) -> pd.DataFrame:
    batch = _force_int_types(batch.copy())

    result = batch.merge(
        zones,
        how="left",
        left_on="PULocationID",
        right_on="LocationID",
        sort=False,
    ).drop(columns=["LocationID"])

    for c in ["pickup_borough", "pickup_zone", "pickup_service_zone"]:
        result[c] = result[c].astype("string")

    return result


def _finish(batch: pd.DataFrame) -> pd.DataFrame:
    # pickup_date comes from the ORIGINAL local timestamp (Spark's to_date uses
    # the session timezone, so it equals the raw date). Shift timestamps after.
    batch["pickup_date"] = batch["pickup_datetime"].dt.strftime("%Y-%m-%d")
    batch["pickup_datetime"] = _to_spark_utc(batch["pickup_datetime"])
    batch["dropoff_datetime"] = _to_spark_utc(batch["dropoff_datetime"])
    batch = _force_int_types(batch)
    return batch[FINAL_COLUMNS].reset_index(drop=True)


def transform_native(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    batch["average_speed_mph"] = (
        batch["trip_distance"].astype("float64")
        * 3600.0
        / batch["duration_seconds"].astype("float64")
    )
    return _finish(batch)


def transform_udf(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    batch["average_speed_mph"] = pd.Series(
        [
            calculate_average_speed(d, s)
            for d, s in zip(batch["trip_distance"], batch["duration_seconds"])
        ],
        index=batch.index,
        dtype="float64",
    )
    return _finish(batch)


# ------------------------------------------------------------
# Zone lookup (mirrors Spark's CSV reading semantics)
# ------------------------------------------------------------
def load_zone_lookup() -> pd.DataFrame:
    zones = pd.read_csv(
        DATA_DIR / "taxi_zone_lookup.csv",
        dtype=str,
        keep_default_na=False,   # keep "N/A", "NA", etc. as literal strings
        na_values=[""],          # only empty string -> null (Spark default)
    )
    zones["LocationID"] = pd.to_numeric(zones["LocationID"], errors="coerce").astype("Int64")
    zones = zones[["LocationID", "Borough", "Zone", "service_zone"]]
    zones = zones.drop_duplicates("LocationID").rename(
        columns={
            "Borough": "pickup_borough",
            "Zone": "pickup_zone",
            "service_zone": "pickup_service_zone",
        }
    )
    for c in ["pickup_borough", "pickup_zone", "pickup_service_zone"]:
        zones[c] = zones[c].astype("string")
    return zones.reset_index(drop=True)


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["native", "udf"], default="native")
    parser.add_argument("--max-months", type=int, default=36)
    parser.add_argument("--output-dir", default=None)
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

    zones = load_zone_lookup()

    if not ray.is_initialized():
        ray.init(address="10.0.2.15:6379")

    # Memory guard-rails for Ray Data (Ray 2.20)
    ctx = rd.DataContext.get_current()
    ctx.target_max_block_size = TARGET_MAX_BLOCK_SIZE
    try:
        ctx.execution_options.resource_limits.object_store_memory = OBJECT_STORE_LIMIT
    except Exception as exc:  # keep running if the attribute is not settable
        print(f"[warn] could not set object_store_memory limit: {exc}")

    transform_fn = transform_native if args.mode == "native" else transform_udf

    start = time.perf_counter()
    month_times = []

    try:
        for index, trip_file in enumerate(trip_files, start=1):
            month = trip_file.stem.replace("yellow_tripdata_", "")
            month_output = output_dir / f"month={month}"
            if month_output.exists():
                shutil.rmtree(month_output)

            month_start = time.perf_counter()
            print(f"\n{'=' * 70}\nSTARTING MONTH {index}/{len(trip_files)}: {month}\n{'=' * 70}")

            # ---------------- Ingestion (only needed columns) ----------------
            available = set(pq.read_schema(str(trip_file)).names)
            cols = [c for c in SOURCE_COLUMNS if c in available]

            ds = rd.read_parquet(
                str(trip_file),
                columns=cols,
                override_num_blocks=READ_BLOCKS,
            )

            # ---------------- Stage 1: normalize + cleanse (distributed) -----
            ds = ds.map_batches(
                normalize_and_clean_batch,
                batch_format="pandas",
                batch_size=MAP_BATCH_SIZE,
                num_cpus=1,
            )
            cleaned = ds.to_pandas(limit=500_000_000)
            del ds

            # ---------------- Exact month-level duplicate removal ------------
            # Equivalent to Spark's df.dropDuplicates() on the whole month.
            cleaned = _force_int_types(cleaned).drop_duplicates()
            cleaned = cleaned.reset_index(drop=True)

            chunks = [
                cleaned.iloc[i : i + OUTPUT_CHUNK_ROWS]
                for i in range(0, len(cleaned), OUTPUT_CHUNK_ROWS)
            ]
            if not chunks:
                chunks = [cleaned]
            ds2 = rd.from_pandas(chunks)
            del cleaned, chunks
            gc.collect()

            # ---------------- Stage 2: join + features (distributed) ---------
            ds2 = ds2.map_batches(
                enrich_with_zones,
                batch_format="pandas",
                batch_size=MAP_BATCH_SIZE,
                num_cpus=1,
                fn_kwargs={"zones": zones},
            )
            ds2 = ds2.map_batches(
                transform_fn,
                batch_format="pandas",
                batch_size=MAP_BATCH_SIZE,
                num_cpus=1,
            )

            # ---------------- Export -----------------------------------------
            ds2.write_parquet(str(month_output))
            del ds2
            gc.collect()

            elapsed = time.perf_counter() - month_start
            month_times.append({"month": month, "seconds": elapsed})
            total_so_far = time.perf_counter() - start
            print(
                f"[{index:02d}/{len(trip_files)}] {month} COMPLETED in {elapsed:.2f}s | "
                f"total {total_so_far:.2f}s ({total_so_far / 60:.2f} min)"
            )
    finally:
        total_elapsed = time.perf_counter() - start

    metrics = {
        "framework": "ray",
        "mode": args.mode,
        "months_processed": len(trip_files),
        "total_wall_clock_seconds": total_elapsed,
        "month_times": month_times,
        "output_dir": str(output_dir),
        "ray_join_note": (
            "Taxi-zone enrichment uses Pandas merge inside Ray map_batches "
            "(zone table passed via fn_kwargs)."
        ),
        "deduplication_note": (
            "Exact per-month drop_duplicates() on the cleaned month, matching "
            "Spark's per-month dropDuplicates(). No global groupby shuffle."
        ),
        "memory_tuning": {
            "read_blocks": READ_BLOCKS,
            "map_batch_size": MAP_BATCH_SIZE,
            "output_chunk_rows": OUTPUT_CHUNK_ROWS,
            "object_store_limit_bytes": OBJECT_STORE_LIMIT,
            "target_max_block_size_bytes": TARGET_MAX_BLOCK_SIZE,
            "columns_projected_at_read": True,
        },
    }
    metrics_path = BENCHMARK_DIR / f"ray_{args.mode}_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2))

    print(f"\n{'=' * 70}\n=== RAY BENCHMARK COMPLETE ===\n{'=' * 70}")
    print(f"Mode: {args.mode}")
    print(f"Months processed: {len(trip_files)}/{len(trip_files)}")
    print(f"Total wall-clock time: {total_elapsed:.2f}s ({total_elapsed / 60:.2f} min)")
    print(f"Output: {output_dir}")
    print(f"Metrics: {metrics_path}")


if __name__ == "__main__":
    main()