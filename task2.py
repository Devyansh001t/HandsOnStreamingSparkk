"""
task2.py
--------
Task 2: Real-Time Aggregations (Driver-Level)
----------------------------------------------
Reads the same socket stream as Task 1, then computes per-driver:
  - total_fare   = SUM(fare_amount)
  - avg_distance = AVG(distance_km)

Results are written to:
  - console       (live view, complete mode so totals update in place)
  - outputs/task_2 (CSV, complete mode via foreachBatch)

Note: complete-mode aggregations require foreachBatch for CSV because
      the built-in CSV sink does not support complete output mode on
      an unbounded stream.
"""

import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["JAVA_TOOL_OPTIONS"] = "--add-opens java.base/javax.security.auth=ALL-UNNAMED"

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, sum as _sum, avg, round as _round
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOCKET_HOST  = "localhost"
SOCKET_PORT  = 9999
OUTPUT_DIR   = os.path.join(os.path.dirname(__file__), "outputs", "task_2")
CHECKPOINT_C = os.path.join(os.path.dirname(__file__), "checkpoints", "task_2_console")
CHECKPOINT_F = os.path.join(os.path.dirname(__file__), "checkpoints", "task_2_csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("RideSharing_Task2_Aggregations")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
ride_schema = StructType([
    StructField("trip_id",     StringType(), True),
    StructField("driver_id",   StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp",   StringType(), True),
])

# ---------------------------------------------------------------------------
# Ingest + parse
# ---------------------------------------------------------------------------
parsed = (
    spark.readStream
    .format("socket")
    .option("host", SOCKET_HOST)
    .option("port", SOCKET_PORT)
    .load()
    .select(from_json(col("value"), ride_schema).alias("data"))
    .select("data.*")
)

# ---------------------------------------------------------------------------
# Aggregation: per-driver totals
# ---------------------------------------------------------------------------
driver_stats = (
    parsed
    .groupBy("driver_id")
    .agg(
        _round(_sum("fare_amount"),  2).alias("total_fare"),
        _round(avg("distance_km"),   2).alias("avg_distance"),
    )
    .orderBy("driver_id")
)

# ---------------------------------------------------------------------------
# Sink 1: console (complete mode — full table reprinted each micro-batch)
# ---------------------------------------------------------------------------
console_query = (
    driver_stats.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_C)
    .start()
)

# ---------------------------------------------------------------------------
# Sink 2: CSV via foreachBatch
#   Each micro-batch snapshot overwrites a single CSV so the file always
#   holds the latest complete aggregation state.
# ---------------------------------------------------------------------------
_batch_counter = [0]

def write_csv_batch(batch_df: DataFrame, batch_id: int):
    """Write the current complete aggregation snapshot to CSV."""
    if batch_df.isEmpty():
        return
    batch_path = os.path.join(OUTPUT_DIR, f"batch_{batch_id:06d}")
    (
        batch_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(batch_path)
    )
    print(f"[Task 2] CSV batch {batch_id} written → {batch_path}")

csv_query = (
    driver_stats.writeStream
    .foreachBatch(write_csv_batch)
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_F)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"[Task 2] Streaming aggregations started.")
print(f"[Task 2] CSV output → {OUTPUT_DIR}")
print("[Task 2] Press Ctrl+C to stop.\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[Task 2] Stopped by user.")
finally:
    for q in spark.streams.active:
        q.stop()
    spark.stop()
