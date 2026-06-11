"""
task3.py
--------
Task 3: Windowed Time-Based Analytics
--------------------------------------
Converts the string timestamp column to TimestampType (event_time),
then performs a 5-minute sliding window aggregation (slides every 1 minute,
watermark of 1 minute) computing SUM(fare_amount) per window.

Results are written to:
  - console        (live view)
  - outputs/task_3 (CSV via foreachBatch)
"""

import os
import sys

# ----------------------------
# CRITICAL WINDOWS & ENVIRONMENT PATH CORRECTIONS
# ----------------------------
# 1. Define explicit home directories
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["SPARK_HOME"] = r"C:\Users\Devyansh Tailor\Documents\Githubfor\HandsOnStreamingSparkk\spark-env\Lib\site-packages\pyspark"

# 2. Reconstruct PATH cleanly in a single execution step
os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep +
    r"C:\hadoop\bin" + os.pathsep +
    os.environ.get("PATH", "")
)

# 3. Security modular patches & Network loopback bind
# os.environ["JAVA_TOOL_OPTIONS"] = "--add-opens java.base/javax.security.auth=ALL-UNNAMED"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# NOW WE IMPORT PYSPARK MODULES SAFELY
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    window, sum as _sum, round as _round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOCKET_HOST  = "127.0.0.1" 
SOCKET_PORT  = 9999
OUTPUT_DIR   = os.path.join(os.path.dirname(__file__), "outputs", "task_3")
CHECKPOINT_C = os.path.join(os.path.dirname(__file__), "checkpoints", "task_3_console")
CHECKPOINT_F = os.path.join(os.path.dirname(__file__), "checkpoints", "task_3_csv")

WINDOW_DURATION = "5 minutes"
SLIDE_DURATION  = "1 minute"
WATERMARK_DELAY = "1 minute"
TIMESTAMP_FMT   = "yyyy-MM-dd HH:mm:ss"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("RideSharing_Task3_WindowedAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2") 
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Schema (Matches your generator exactly)
# ---------------------------------------------------------------------------
ride_schema = StructType([
    StructField("trip_id",     StringType(), True),
    StructField("driver_id",   IntegerType(), True), 
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp",   StringType(), True),
])

# ---------------------------------------------------------------------------
# Ingest + parse + cast timestamp
# ---------------------------------------------------------------------------
parsed = (
    spark.readStream
    .format("socket")
    .option("host", SOCKET_HOST)
    .option("port", SOCKET_PORT)
    .load()
    .select(from_json(col("value"), ride_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), TIMESTAMP_FMT))
    .drop("timestamp")
)

# ---------------------------------------------------------------------------
# Apply watermark, then 5-minute sliding window on event_time
# ---------------------------------------------------------------------------
windowed_agg = (
    parsed
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION)
    )
    .agg(
        _round(_sum("fare_amount"), 2).alias("total_fare")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare"),
    )
)

# ---------------------------------------------------------------------------
# Sink 1: console
# ---------------------------------------------------------------------------
console_query = (
    windowed_agg.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 30)
    .outputMode("append") 
    .option("checkpointLocation", CHECKPOINT_C)
    .start()
)

# ---------------------------------------------------------------------------
# Sink 2: CSV via foreachBatch
# ---------------------------------------------------------------------------
def write_csv_batch(batch_df: DataFrame, batch_id: int):
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
    print(f"[Task 3] CSV batch {batch_id} written → {batch_path}")

csv_query = (
    windowed_agg.writeStream
    .foreachBatch(write_csv_batch)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_F)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"[Task 3] Windowed streaming started ({WINDOW_DURATION} window, {SLIDE_DURATION} slide).")
print(f"[Task 3] CSV output → {OUTPUT_DIR}")
print("[Task 3] Note: windowed results appear only after the watermark advances.")
print("[Task 3] Press Ctrl+C to stop.\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[Task 3] Stopped by user.")
finally:
    for q in spark.streams.active:
        q.stop()
    spark.stop()