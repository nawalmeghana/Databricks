# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest raw CSV files into Delta Lake bronze tables with no transformations.
# MAGIC Each table receives metadata columns for lineage and auditability.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `bronze_signal_metrics`
# MAGIC - `bronze_5g_network`
# MAGIC - `bronze_customer_churn`

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime
import uuid

# Initialize Spark (already available in Databricks as `spark`)
spark = SparkSession.builder \
    .appName("TelecomLakehouse-BronzeIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configuration
RAW_DATA_PATH = "/FileStore/telecom/raw"          # DBFS path for uploaded CSVs
BRONZE_PATH   = "/FileStore/telecom/bronze"       # Delta output path
DATABASE_NAME = "telecom_lakehouse"
BATCH_ID      = str(uuid.uuid4())[:8]             # Unique batch identifier

print(f"Batch ID: {BATCH_ID}")
print(f"Ingestion started at: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")
print(f"Using database: {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Signal Metrics (signal_metrics.csv)

# COMMAND ----------

# Define schema for signal_metrics.csv
# Columns: Timestamp, Locality, Latitude, Longitude, Signal Strength (dBm),
#          Signal Quality (%), Data Throughput (Mbps), Latency (ms), Network Type,
#          BB60C Measurement (dBm), srsRAN Measurement (dBm), BladeRFxA9 Measurement (dBm)

signal_metrics_schema = StructType([
    StructField("Timestamp",                StringType(), True),
    StructField("Locality",                 StringType(), True),
    StructField("Latitude",                 DoubleType(), True),
    StructField("Longitude",                DoubleType(), True),
    StructField("Signal Strength (dBm)",    DoubleType(), True),
    StructField("Signal Quality (%)",       DoubleType(), True),
    StructField("Data Throughput (Mbps)",   DoubleType(), True),
    StructField("Latency (ms)",             DoubleType(), True),
    StructField("Network Type",             StringType(), True),
    StructField("BB60C Measurement (dBm)",  DoubleType(), True),
    StructField("srsRAN Measurement (dBm)", DoubleType(), True),
    StructField("BladeRFxA9 Measurement (dBm)", DoubleType(), True),
])

df_signal_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(signal_metrics_schema) \
    .csv(f"{RAW_DATA_PATH}/signal_metrics.csv")

# Add bronze metadata columns
df_signal_bronze = df_signal_raw \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", input_file_name()) \
    .withColumn("_batch_id", lit(BATCH_ID)) \
    .withColumn("_layer", lit("bronze"))

print(f"Signal Metrics rows loaded: {df_signal_bronze.count()}")
df_signal_bronze.printSchema()

# COMMAND ----------

# Write to Delta — Bronze Signal Metrics
df_signal_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("Network Type") \
    .save(f"{BRONZE_PATH}/signal_metrics")

# Register as table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_signal_metrics
    USING DELTA
    LOCATION '{BRONZE_PATH}/signal_metrics'
""")

print("✅ bronze_signal_metrics written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest 5G Network Data (5g_network_data.csv)

# COMMAND ----------

# Define schema for 5g_network_data.csv
# Columns: Timestamp, Location, Signal Strength (dBm), Download Speed (Mbps),
#          Upload Speed (Mbps), Latency (ms), Jitter (ms), Network Type,
#          Device Model, Carrier, Band, Battery Level (%), Temperature (°C),
#          Connected Duration (min), Handover Count, Data Usage (MB),
#          Video Streaming Quality, VoNR Enabled, Network Congestion Level,
#          Ping to Google (ms), Dropped Connection

network_5g_schema = StructType([
    StructField("Timestamp",                StringType(),  True),
    StructField("Location",                 StringType(),  True),
    StructField("Signal Strength (dBm)",    DoubleType(),  True),
    StructField("Download Speed (Mbps)",    DoubleType(),  True),
    StructField("Upload Speed (Mbps)",      DoubleType(),  True),
    StructField("Latency (ms)",             DoubleType(),  True),
    StructField("Jitter (ms)",              DoubleType(),  True),
    StructField("Network Type",             StringType(),  True),
    StructField("Device Model",             StringType(),  True),
    StructField("Carrier",                  StringType(),  True),
    StructField("Band",                     StringType(),  True),
    StructField("Battery Level (%)",        DoubleType(),  True),
    StructField("Temperature (°C)",         DoubleType(),  True),
    StructField("Connected Duration (min)", DoubleType(),  True),
    StructField("Handover Count",           IntegerType(), True),
    StructField("Data Usage (MB)",          DoubleType(),  True),
    StructField("Video Streaming Quality",  IntegerType(), True),
    StructField("VoNR Enabled",             StringType(),  True),
    StructField("Network Congestion Level", StringType(),  True),
    StructField("Ping to Google (ms)",      DoubleType(),  True),
    StructField("Dropped Connection",       StringType(),  True),
])

df_5g_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(network_5g_schema) \
    .csv(f"{RAW_DATA_PATH}/5g_network_data.csv")

df_5g_bronze = df_5g_raw \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", input_file_name()) \
    .withColumn("_batch_id", lit(BATCH_ID)) \
    .withColumn("_layer", lit("bronze"))

print(f"5G Network rows loaded: {df_5g_bronze.count()}")
df_5g_bronze.printSchema()

# COMMAND ----------

df_5g_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("Network Type") \
    .save(f"{BRONZE_PATH}/5g_network")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_5g_network
    USING DELTA
    LOCATION '{BRONZE_PATH}/5g_network'
""")

print("✅ bronze_5g_network written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Customer Churn Data (telco_customer_churn.csv)

# COMMAND ----------

churn_schema = StructType([
    StructField("customerID",        StringType(),  True),
    StructField("gender",            StringType(),  True),
    StructField("SeniorCitizen",     IntegerType(), True),
    StructField("Partner",           StringType(),  True),
    StructField("Dependents",        StringType(),  True),
    StructField("tenure",            IntegerType(), True),
    StructField("PhoneService",      StringType(),  True),
    StructField("MultipleLines",     StringType(),  True),
    StructField("InternetService",   StringType(),  True),
    StructField("OnlineSecurity",    StringType(),  True),
    StructField("OnlineBackup",      StringType(),  True),
    StructField("DeviceProtection",  StringType(),  True),
    StructField("TechSupport",       StringType(),  True),
    StructField("StreamingTV",       StringType(),  True),
    StructField("StreamingMovies",   StringType(),  True),
    StructField("Contract",          StringType(),  True),
    StructField("PaperlessBilling",  StringType(),  True),
    StructField("PaymentMethod",     StringType(),  True),
    StructField("MonthlyCharges",    DoubleType(),  True),
    StructField("TotalCharges",      StringType(),  True),  # Has spaces in raw data
    StructField("Churn",             StringType(),  True),
])

df_churn_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(churn_schema) \
    .csv(f"{RAW_DATA_PATH}/telco_customer_churn.csv")

df_churn_bronze = df_churn_raw \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", input_file_name()) \
    .withColumn("_batch_id", lit(BATCH_ID)) \
    .withColumn("_layer", lit("bronze"))

print(f"Customer Churn rows loaded: {df_churn_bronze.count()}")
df_churn_bronze.printSchema()

# COMMAND ----------

df_churn_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{BRONZE_PATH}/customer_churn")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_customer_churn
    USING DELTA
    LOCATION '{BRONZE_PATH}/customer_churn'
""")

print("✅ bronze_customer_churn written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Row Counts

# COMMAND ----------

tables = ["bronze_signal_metrics", "bronze_5g_network", "bronze_customer_churn"]
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.{table}").collect()[0]["cnt"]
    print(f"  {table}: {count:,} rows")

print("\n✅ Bronze ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table History (Audit Log)

# COMMAND ----------

# View Delta history for auditability
spark.sql(f"DESCRIBE HISTORY {DATABASE_NAME}.bronze_signal_metrics").show(5, truncate=False)
