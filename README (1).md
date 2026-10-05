# DA3408 Assignment 3 — Spark vs. Ray

## Overview
This repository implements the DA3408 Assignment 3 preprocessing pipeline in Apache Spark and Ray Data on 36 months of NYC Yellow Taxi data.

Pipeline:
1. Ingestion of monthly Parquet files.
2. Null/invalid filtering, timestamp normalization and duplicate removal.
3. Heavy join with `taxi_zone_lookup.csv`.
4. `average_speed_mph` derived feature.
5. Parquet export.

## Repository policy
The two large directories below are intentionally excluded from Git:

```text
data/
output/
```

Everything else can be committed, subject to GitHub's individual-file limits.

## Repository layout

```text
AIOps_Assignment3/
├── README.md
├── .gitignore
├── scripts/
│   ├── spark_clean.py
│   └── ray_clean.py
├── benchmark/
├── logs/
├── screenshots/
├── report/
│   ├── report.tex
│   └── figures/
└── ...
```

## Environment used

- Ubuntu 24.04.4 LTS ARM64 VM
- Python 3.10.13
- Java 17
- Apache Spark 4.2.0
- Ray 2.20.0
- PyArrow 15.0.2
- Ray Data
- VM IP during the experiment: `10.0.2.15`

## 1. Clone

```bash
git clone https://github.com/Ramanagopalkodali/AIOps_Assignment3.git
cd AIOps_Assignment3
```

## 2. Dataset

The NYC Taxi input data is not committed. Create:

```bash
mkdir -p data
```

Place the 36 Yellow Taxi monthly Parquet files in `data/`:

```text
yellow_tripdata_2023-01.parquet
...
yellow_tripdata_2025-12.parquet
```

Also place:

```text
data/taxi_zone_lookup.csv
```

The scripts discover files matching `yellow_tripdata_*.parquet`.

## 3. Spark cluster

Set:

```bash
export SPARK_HOME=$HOME/spark-4.2.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export SPARK_LOCAL_IP=<VM_IP>
```

Configure the master with:

```text
SPARK_MASTER_HOST=<VM_IP>
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
```

Start the master:

```bash
$SPARK_HOME/sbin/start-master.sh
```

Start worker 1:

```bash
$SPARK_HOME/sbin/start-worker.sh spark://<VM_IP>:7077
```

Start worker 2:

```bash
mkdir -p /tmp/spark-worker2

SPARK_PID_DIR=/tmp/spark-worker2 \
SPARK_WORKER_PORT=7078 \
SPARK_WORKER_WEBUI_PORT=8082 \
SPARK_WORKER_DIR=/tmp/spark-worker2 \
$SPARK_HOME/sbin/start-worker.sh spark://<VM_IP>:7077
```

Check the two workers at:

```text
http://<VM_IP>:8080
```

## 4. Spark Native

```bash
python scripts/spark_clean.py --mode native --max-months 36
```

## 5. Spark UDF

```bash
python scripts/spark_clean.py --mode udf --max-months 36
```

## 6. Ray cluster

Start the Ray head:

```bash
ray start --head \
  --node-ip-address=<VM_IP> \
  --port=6379 \
  --dashboard-host=0.0.0.0 \
  --dashboard-port=8265 \
  --num-cpus=1
```

Start the two worker processes/nodes against the head:

```bash
ray start --address='<VM_IP>:6379' \
  --node-ip-address=<WORKER_IP> \
  --num-cpus=1
```

Check:

```text
http://<VM_IP>:8265
```

## 7. Ray Native

```bash
python scripts/ray_clean.py \
  --mode native \
  --max-months 36 \
  --output-dir output/ray_result_native
```

## 8. Ray UDF

```bash
python scripts/ray_clean.py \
  --mode udf \
  --max-months 36 \
  --output-dir output/ray_result_udf
```

## 9. Resource monitoring

During each run:

```bash
top -b -n 1
free -m
```

The supplied logs contain the recorded samples. Peak CPU is calculated as `100 - idle%`. Peak RAM is the maximum `used` value reported by `free -m`.

## 10. Recorded results

| Framework | Mode | Wall-clock |
|---|---|---:|
| Spark | Native | 392.50 s (6.54 min) |
| Spark | UDF | 432.34 s (7.21 min) |
| Ray | Native | 636.96 s (10.62 min) |
| Ray | UDF | 711.03 s (11.85 min) |

UDF overhead:
- Spark: **10.15%**
- Ray: **11.63%**

## 11. Correctness

The final Ray implementation was made parity-focused: month-level exact deduplication, Spark-compatible whole-second duration, nullable integer columns, compatible taxi-zone string handling, stable string types, and the same average-speed expression.

The completed main parity comparison reported **127,627,157 rows across 36 months** with matching logical results in the completed checks.

## 12. Report

The report source is:

```text
report/report.tex
```

Plots are in:

```text
report/figures/
```

The report can be uploaded directly to Overleaf. The plots are already generated from the supplied benchmark metric files and resource logs.

## 13. AI usage

The report includes an AI Usage Appendix covering permitted code translation/adaptation, debugging, memory-safe optimization, parity-check construction, benchmark analysis, and documentation. Cluster/network configuration was performed manually. Reported benchmark values are from actual recorded executions.

## 14. Reproduction

The generated `output/` directory is intentionally not committed. Running the four commands above recreates the outputs locally after the dataset is placed in `data/`.
