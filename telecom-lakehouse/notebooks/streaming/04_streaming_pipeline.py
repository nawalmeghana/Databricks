# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 Streaming Pipeline — Real-Time Network Event Simulation
# MAGIC
# MAGIC **Purpose:** Simulate real-time incoming telecom network events using
# MAGIC Spark Structured Streaming. Demonstrates how the platform handles
# MAGIC live data alongside batch ingestion.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC [Rate Source / File Stream] → Bronze Delta → Silver Delta → Gold Delta
# MAGIC ```

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, expr, rand, randn,
    when, round as spark_round, concat, lpad,
    from_json, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
import time

spark = SparkSession.builder \
    .appName("TelecomLakehouse-StreamingPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

DATABASE_NAME    = "telecom_lakehouse"
STREAMING_CHKPT  = "/FileStore/telecom/checkpoints"
BRONZE_STREAM_PATH = "/FileStore/telecom/bronze/stream_network"

spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Synthetic Streaming Network Events (Rate Source)
# MAGIC
# MAGIC The `rate` source generates rows at a specified rate — ideal for simulating
# MAGIC real-time network telemetry without an actual Kafka broker in a demo environment.

# COMMAND ----------

# Define locations and network types for simulation
LOCATIONS     = ["San Francisco", "New York", "Chicago", "Houston", "Phoenix",
                 "Seattle", "Austin", "Boston", "Denver", "Atlanta"]
NETWORK_TYPES = ["5G", "5G NSA", "4G", "LTE"]
CARRIERS      = ["AT&T", "Verizon", "T-Mobile", "Dish"]
DEVICES       = ["iPhone 14", "Pixel 7", "Samsung S23", "OnePlus 11"]
CONGESTION    = ["Low", "Medium", "High"]
BANDS         = ["n78", "n260", "n41", "n66", "B4"]

# Use rate source to simulate ~2 events/second
streaming_raw = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

# Enrich with simulated telecom metrics using random expressions
streaming_events = streaming_raw \
    .withColumn("event_id",        col("value")) \
    .withColumn("event_timestamp", col("timestamp")) \
    .withColumn("location",        expr(f"element_at(array({', '.join(repr(l) for l in LOCATIONS)}), cast(rand() * {len(LOCATIONS)} + 1 AS INT))")) \
    .withColumn("network_type",    expr(f"element_at(array({', '.join(repr(n) for n in NETWORK_TYPES)}), cast(rand() * {len(NETWORK_TYPES)} + 1 AS INT))")) \
    .withColumn("carrier",         expr(f"element_at(array({', '.join(repr(c) for c in CARRIERS)}), cast(rand() * {len(CARRIERS)} + 1 AS INT))")) \
    .withColumn("device_model",    expr(f"element_at(array({', '.join(repr(d) for d in DEVICES)}), cast(rand() * {len(DEVICES)} + 1 AS INT))")) \
    .withColumn("congestion_level",expr(f"element_at(array({', '.join(repr(c) for c in CONGESTION)}), cast(rand() * {len(CONGESTION)} + 1 AS INT))")) \
    .withColumn("band",            expr(f"element_at(array({', '.join(repr(b) for b in BANDS)}), cast(rand() * {len(BANDS)} + 1 AS INT))")) \
    .withColumn("signal_strength_dbm",   spark_round(-120 + rand() * 80, 1)) \
    .withColumn("download_mbps",          spark_round(50 + rand() * 900, 2)) \
    .withColumn("upload_mbps",            spark_round(10 + rand() * 200, 2)) \
    .withColumn("latency_ms",             spark_round(5  + rand() * 150, 1)) \
    .withColumn("jitter_ms",              spark_round(1  + rand() * 20,  1)) \
    .withColumn("ping_google_ms",         spark_round(10 + rand() * 100, 1)) \
    .withColumn("dropped_connection",     (rand() > 0.92)) \
    .withColumn("vonr_enabled",           (rand() > 0.5)) \
    .withColumn("battery_pct",            spark_round(10 + rand() * 90,  1)) \
    .withColumn("temperature_c",          spark_round(15 + rand() * 30,  1)) \
    .withColumn("connected_duration_min", spark_round(1  + rand() * 120, 1)) \
    .withColumn("handover_count",         (rand() * 5).cast(IntegerType())) \
    .withColumn("data_usage_mb",          spark_round(10 + rand() * 500, 2)) \
    .withColumn("video_quality_score",    (1 + rand() * 4).cast(IntegerType())) \
    .withColumn("_source", lit("streaming_simulation")) \
    .drop("value", "timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Stream to Bronze Delta Table (Append Mode)

# COMMAND ----------

# Stream → Bronze Delta (append-only)
query_bronze = streaming_events.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{STREAMING_CHKPT}/bronze_stream") \
    .trigger(processingTime="10 seconds") \
    .start(BRONZE_STREAM_PATH)

print(f"✅ Bronze streaming query started: {query_bronze.id}")
print(f"   Status: {query_bronze.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Stream from Bronze → Silver (Continuous Transformation)
# MAGIC
# MAGIC A second streaming query reads from the bronze Delta table and applies
# MAGIC Silver-layer transformations, writing cleaned events to silver.

# COMMAND ----------

SILVER_STREAM_PATH = "/FileStore/telecom/silver/stream_network"

# Read from bronze Delta stream
df_bronze_stream = spark.readStream \
    .format("delta") \
    .load(BRONZE_STREAM_PATH)

# Apply Silver transformations
df_silver_stream = df_bronze_stream \
    .filter(
        col("latency_ms").isNotNull() &
        col("download_mbps").isNotNull() &
        (col("latency_ms") > 0) &
        (col("download_mbps") > 0)
    ) \
    .withColumn(
        "signal_category",
        when(col("signal_strength_dbm") >= -80,  "Excellent")
        .when(col("signal_strength_dbm") >= -90,  "Good")
        .when(col("signal_strength_dbm") >= -100, "Fair")
        .otherwise("Poor")
    ) \
    .withColumn(
        "performance_tier",
        when(col("download_mbps") >= 500, "Ultra")
        .when(col("download_mbps") >= 100, "High")
        .when(col("download_mbps") >= 25,  "Medium")
        .otherwise("Low")
    ) \
    .withColumn("_silver_timestamp", current_timestamp())

# Write to Silver Delta stream
query_silver = df_silver_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{STREAMING_CHKPT}/silver_stream") \
    .trigger(processingTime="30 seconds") \
    .start(SILVER_STREAM_PATH)

print(f"✅ Silver streaming query started: {query_silver.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Stream to Console (Debug / Monitoring)
# MAGIC
# MAGIC Useful during development to see events as they arrive.

# COMMAND ----------

query_console = streaming_events \
    .select("event_timestamp", "location", "network_type", "download_mbps", "latency_ms", "dropped_connection") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("numRows", 5) \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

# Let it run for 60 seconds then stop for demo
time.sleep(60)
query_console.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monitor Streaming Queries

# COMMAND ----------

def print_stream_status(query, name):
    status = query.status
    progress = query.lastProgress
    print(f"\n📊 {name}:")
    print(f"   Active: {query.isActive}")
    print(f"   Status: {status.get('message', 'N/A')}")
    if progress:
        print(f"   Input rows/sec: {progress.get('inputRowsPerSecond', 0):.1f}")
        print(f"   Processed rows/sec: {progress.get('processedRowsPerSecond', 0):.1f}")

print_stream_status(query_bronze, "Bronze Stream")
print_stream_status(query_silver, "Silver Stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Stop Streams (run when done)

# COMMAND ----------

# Uncomment to stop all streams
# query_bronze.stop()
# query_silver.stop()
# spark.streams.resetTerminated()
# print("All streams stopped.")
