"""
task1.py
--------
Task 1: Basic Streaming Ingestion and Parsing
----------------------------------------------
Reads JSON ride records from a socket stream (localhost:9999),
parses them into a typed DataFrame, and writes the results to:
  - console  (for live monitoring)
  - outputs/task_1  (CSV, for submission)

Schema: trip_id (string), driver_id (string), distance_km (double),
        fare_amount (double), timestamp (string)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)
import os

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOCKET_HOST  = "localhost"
SOCKET_PORT  = 9999
OUTPUT_DIR   = os.path.join(os.path.dirname(__file__), "outputs", "task_1")
CHECKPOINT   = os.path.join(os.path.dirname(__file__), "checkpoints", "task_1")

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("RideSharing_Task1_Ingestion")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Define schema matching data_generator.py output
# ---------------------------------------------------------------------------
ride_schema = StructType([
    StructField("trip_id",     StringType(), True),
    StructField("driver_id",   StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp",   StringType(), True),
])

# ---------------------------------------------------------------------------
# Read raw text stream from socket
# ---------------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("socket")
    .option("host", SOCKET_HOST)
    .option("port", SOCKET_PORT)
    .load()
)

# ---------------------------------------------------------------------------
# Parse JSON payload → typed columns
# ---------------------------------------------------------------------------
parsed = (
    raw_stream
    .select(from_json(col("value"), ride_schema).alias("data"))
    .select("data.*")
)

# ---------------------------------------------------------------------------
# Sink 1: console (live view)
# ---------------------------------------------------------------------------
console_query = (
    parsed.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .outputMode("append")
    .start()
)

# ---------------------------------------------------------------------------
# Sink 2: CSV files in outputs/task_1
# ---------------------------------------------------------------------------
csv_query = (
    parsed.writeStream
    .format("csv")
    .option("path", OUTPUT_DIR)
    .option("checkpointLocation", CHECKPOINT)
    .option("header", True)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"[Task 1] Streaming started. Parsed records will appear above.")
print(f"[Task 1] CSV output → {OUTPUT_DIR}")
print("[Task 1] Press Ctrl+C to stop.\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[Task 1] Stopped by user.")
finally:
    for q in spark.streams.active:
        q.stop()
    spark.stop()
