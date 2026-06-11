"""
Task 1 - Fixed Windows Stable Version
--------------------------------------
Streaming ingestion from socket → JSON parsing → console + CSV output
"""

import os
import sys

# ----------------------------
# CRITICAL WINDOWS & ENVIRONMENT PATH CORRECTIONS
# ----------------------------
# 1. Point directly to your verified pyspark library folder
os.environ["SPARK_HOME"] = r"C:\Users\Devyansh Tailor\Documents\Githubfor\HandsOnStreamingSparkk\spark-env\Lib\site-packages\pyspark"

# 2. Point to your Hadoop winutils folder
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# 3. Reconstruct your PATH variable so Windows finds the binaries instantly
os.environ["PATH"] = (
    os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep +
    r"C:\hadoop\bin" + os.pathsep +
    os.environ.get("PATH", "")
)

# 4. Bind strictly to loopback IP to avoid Windows environment clashing
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


# NOW WE IMPORT PYSPARK MODULES SAFELY
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


# ----------------------------
# CONFIG
# ----------------------------
# Using 127.0.0.1 matches our fixed data generator exactly
SOCKET_HOST = "127.0.0.1"
SOCKET_PORT = 9999

OUTPUT_DIR = "C:/tmp/outputs/task_1"
CHECKPOINT_DIR = "C:/tmp/checkpoints/task_1"
CONSOLE_CHECKPOINT_DIR = "C:/tmp/checkpoints/task_1_console"


# ----------------------------
# SPARK SESSION
# ----------------------------
spark = (
    SparkSession.builder
    .appName("RideSharing_Task1_Safe")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ----------------------------
# SCHEMA (Matches your generator exactly)
# ----------------------------
ride_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True), # Fixed to Integer to match your script's random.randint()
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


# ----------------------------
# STREAM SOURCE
# ----------------------------
raw_stream = (
    spark.readStream
    .format("socket")
    .option("host", SOCKET_HOST)
    .option("port", SOCKET_PORT)
    .load()
)


# ----------------------------
# PARSE JSON
# ----------------------------
parsed = (
    raw_stream
    .select(from_json(col("value"), ride_schema).alias("data"))
    .select("data.*")
)


# ----------------------------
# CONSOLE SINK
# ----------------------------
console_query = (
    parsed.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .option("checkpointLocation", CONSOLE_CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)


# ----------------------------
# CSV SINK
# ----------------------------
csv_query = (
    parsed.writeStream
    .format("csv")
    .option("path", OUTPUT_DIR)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .option("header", True)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)


# ----------------------------
# RUN STREAM
# ----------------------------
print("[Task 1] Streaming started...")
print("[Task 1] Output →", OUTPUT_DIR)
print("[Task 1] Checkpoint →", CHECKPOINT_DIR)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[Task 1] Stopping stream...")
finally:
    for q in spark.streams.active:
        q.stop()
    spark.stop()