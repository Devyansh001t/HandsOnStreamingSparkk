# Ride-Sharing Analytics Using Spark Streaming and Spark SQL

Real-time analytics pipeline for a ride-sharing platform built with **Apache Spark Structured Streaming** and **PySpark**.

---

## Project Structure

```
ride-sharing-analytics/
├── data_generator.py   # TCP socket server that emits synthetic ride records
├── task1.py            # Task 1 – Streaming ingestion & parsing
├── task2.py            # Task 2 – Driver-level real-time aggregations
├── task3.py            # Task 3 – 5-minute sliding-window analytics
├── outputs/
│   ├── task_1/         # CSV output from Task 1
│   ├── task_2/         # CSV output from Task 2
│   └── task_3/         # CSV output from Task 3
└── README.md
```

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.8 – 3.11 | https://www.python.org/downloads/ |
| PySpark | ≥ 3.4 | `pip install pyspark` |
| Faker | any | `pip install faker` |
| Java (JDK) | 11 or 17 | Required by Spark |

> **Windows note:** Spark on Windows requires `winutils.exe`. Download the matching version from https://github.com/steveloughran/winutils and set `HADOOP_HOME`.

---

## Setup

```bash
pip install pyspark faker
```

---

## Running the Pipeline

Each task reads from the **same socket** (`localhost:9999`). You need two terminals open for each task: one running the generator, one running the task.

### Step 1 – Start the data generator (keep running)

```bash
python data_generator.py
```

You will see:
```
[data_generator] Listening on localhost:9999 — waiting for a Spark connection...
```

### Step 2 – Run a task (in a second terminal)

```bash
# Task 1 – Ingest and parse
python task1.py

# Task 2 – Driver-level aggregations
python task2.py

# Task 3 – Windowed analytics
python task3.py
```

> Run one task at a time (each task binds to port 9999 as the single consumer). Restart `data_generator.py` between tasks.

---

## Task Descriptions

### Task 1 – Basic Streaming Ingestion and Parsing

**Goal:** Read raw JSON ride records from the socket, parse them into a typed Spark DataFrame, and surface the results.

**Schema produced:**

| Column | Type |
|--------|------|
| `trip_id` | StringType |
| `driver_id` | StringType |
| `distance_km` | DoubleType |
| `fare_amount` | DoubleType |
| `timestamp` | StringType |

**Implementation highlights:**
- `spark.readStream.format("socket")` ingests one JSON line per message.
- `from_json()` with an explicit `StructType` schema converts the raw string into typed columns.
- Two sinks run in parallel: **console** (live) and **CSV** (`outputs/task_1/`).

---

### Task 2 – Real-Time Aggregations (Driver-Level)

**Goal:** Continuously compute per-driver totals from the running stream.

**Aggregations:**

| Column | Expression |
|--------|-----------|
| `total_fare` | `SUM(fare_amount)` |
| `avg_distance` | `AVG(distance_km)` |

**Implementation highlights:**
- `groupBy("driver_id")` with `complete` output mode so every batch reprints the full updated table.
- CSV sink uses `foreachBatch` because the built-in CSV connector does not support `complete` mode on unbounded streams. Each micro-batch snapshot is written to a separate subfolder under `outputs/task_2/`.

---

### Task 3 – Windowed Time-Based Analytics

**Goal:** Detect fare trends over time using a sliding window on the event timestamp.

**Window parameters:**

| Parameter | Value |
|-----------|-------|
| Window duration | 5 minutes |
| Slide interval | 1 minute |
| Watermark delay | 1 minute |

**Aggregation:** `SUM(fare_amount)` per window, output columns: `window_start`, `window_end`, `total_fare`.

**Implementation highlights:**
- `to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")` casts the string column to `TimestampType` as `event_time`.
- `withWatermark("event_time", "1 minute")` tells Spark how late data can arrive before a window is finalized.
- `window(col("event_time"), "5 minutes", "1 minute")` applies the sliding aggregation.
- Output mode is `append` (windows are emitted only after they are closed by the watermark).
- CSV sink uses `foreachBatch`, writing each finalized window batch to `outputs/task_3/`.

---

## Output Files

After running each task, inspect the CSV output:

```bash
# List all output directories
ls outputs/task_1/
ls outputs/task_2/
ls outputs/task_3/
```

Each subdirectory contains one or more `part-*.csv` files produced by Spark, plus a `_SUCCESS` marker.

---

## Stopping

Press **Ctrl+C** in each terminal window. The scripts handle `KeyboardInterrupt` cleanly and stop all active streaming queries before exiting.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `Connection refused` on port 9999 | Start `data_generator.py` first and wait for the "Listening…" message. |
| `Python 3.13 not supported` | Use Python 3.11: `py -3.11 task1.py` (Windows) or `python3.11 task1.py` (Mac/Linux). |
| Windowed results (Task 3) not appearing | Wait ~2–3 minutes; windows only emit after the watermark advances past their end time. |
| `JAVA_HOME not set` | Install JDK 11/17 and set the `JAVA_HOME` environment variable. |
| `winutils` errors on Windows | Download `winutils.exe` for your Hadoop version and set `HADOOP_HOME`. |

---

## Author

ITCS 6190 – Cloud Computing for Data Analysis  
Ride-Sharing Analytics Assignment
