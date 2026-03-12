# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Data Cleaning & Transformation
# MAGIC
# MAGIC **Purpose:** Transform bronze raw data into clean, typed, validated silver tables.
# MAGIC
# MAGIC **Transformations per table:**
# MAGIC - Parse timestamps to proper types
# MAGIC - Normalize column names (snake_case)
# MAGIC - Remove nulls and duplicates
# MAGIC - Validate value ranges (signal strength, latency, etc.)
# MAGIC - Add derived/enriched columns
# MAGIC - Log data quality failures
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `silver_signal_metrics`
# MAGIC - `silver_network_performance`
# MAGIC - `silver_customer`

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, trim, upper, lower,
    regexp_replace, round as spark_round, lit,
    current_timestamp, coalesce, abs as spark_abs,
    year, month, dayofmonth, hour, date_format,
    count, isnan, isnull
)
from pyspark.sql.types import DoubleType, BooleanType, IntegerType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("TelecomLakehouse-SilverTransformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

DATABASE_NAME = "telecom_lakehouse"
SILVER_PATH   = "/FileStore/telecom/silver"
spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility: Data Quality Logger

# COMMAND ----------

def log_dq_check(table_name, check_name, passed, failed, total):
    """Log data quality check results."""
    pct_pass = round((passed / total) * 100, 2) if total > 0 else 0
    status = "✅ PASS" if failed == 0 else ("⚠️ WARN" if pct_pass > 95 else "❌ FAIL")
    print(f"  {status} [{table_name}] {check_name}: {passed:,}/{total:,} passed ({pct_pass}%)")

def count_nulls(df, col_name):
    return df.filter(col(col_name).isNull() | isnan(col(col_name))).count() \
           if col_name in [f.dataType.simpleString() for f in df.schema.fields] \
           else df.filter(col(col_name).isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Signal Metrics

# COMMAND ----------

df_signal_bronze = spark.table(f"{DATABASE_NAME}.bronze_signal_metrics")
total_signal = df_signal_bronze.count()
print(f"Bronze signal rows: {total_signal:,}")

# ── Step 1: Rename columns to snake_case ──────────────────────────────────────
df_signal = df_signal_bronze \
    .withColumnRenamed("Timestamp",                    "timestamp_raw") \
    .withColumnRenamed("Locality",                     "locality") \
    .withColumnRenamed("Latitude",                     "latitude") \
    .withColumnRenamed("Longitude",                    "longitude") \
    .withColumnRenamed("Signal Strength (dBm)",        "signal_strength_dbm") \
    .withColumnRenamed("Signal Quality (%)",           "signal_quality_pct") \
    .withColumnRenamed("Data Throughput (Mbps)",       "throughput_mbps") \
    .withColumnRenamed("Latency (ms)",                 "latency_ms") \
    .withColumnRenamed("Network Type",                 "network_type") \
    .withColumnRenamed("BB60C Measurement (dBm)",      "bb60c_dbm") \
    .withColumnRenamed("srsRAN Measurement (dBm)",     "srsran_dbm") \
    .withColumnRenamed("BladeRFxA9 Measurement (dBm)", "bladerfxa9_dbm")

# ── Step 2: Parse timestamp ───────────────────────────────────────────────────
df_signal = df_signal.withColumn(
    "event_timestamp",
    to_timestamp(col("timestamp_raw"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
)

# ── Step 3: Add date parts for partitioning / time-series analysis ────────────
df_signal = df_signal \
    .withColumn("event_date",  date_format("event_timestamp", "yyyy-MM-dd")) \
    .withColumn("event_hour",  hour("event_timestamp")) \
    .withColumn("event_month", month("event_timestamp")) \
    .withColumn("event_year",  year("event_timestamp"))

# ── Step 4: Classify signal strength (telecom industry standard) ──────────────
# RSRP equivalent ranges: Excellent > -80, Good -80 to -90, Fair -90 to -100, Poor < -100
df_signal = df_signal.withColumn(
    "signal_category",
    when(col("signal_strength_dbm") >= -80,  "Excellent")
    .when(col("signal_strength_dbm") >= -90,  "Good")
    .when(col("signal_strength_dbm") >= -100, "Fair")
    .otherwise("Poor")
)

# ── Step 5: Normalize network type ───────────────────────────────────────────
df_signal = df_signal.withColumn(
    "network_type_clean",
    when(upper(col("network_type")).isin("5G", "5G NSA", "5G SA"), "5G")
    .when(upper(col("network_type")).isin("4G", "LTE"),             "4G/LTE")
    .when(upper(col("network_type")).isin("3G"),                    "3G")
    .otherwise(col("network_type"))
)

# ── Step 6: Add composite signal quality score (0–100) ───────────────────────
# Normalise signal strength: -140 (worst) to -40 (best) → 0 to 100
df_signal = df_signal.withColumn(
    "signal_score",
    spark_round(
        ((col("signal_strength_dbm") + 140) / 100.0 * 100).cast(DoubleType()),
        2
    )
)

# COMMAND ----------

# ── Data Quality Checks ───────────────────────────────────────────────────────
print("\n📋 Data Quality Checks — Signal Metrics:")

# Remove nulls on critical columns
null_before = df_signal.filter(
    col("signal_strength_dbm").isNull() |
    col("latency_ms").isNull() |
    col("locality").isNull() |
    col("event_timestamp").isNull()
).count()

df_signal_clean = df_signal.filter(
    col("signal_strength_dbm").isNotNull() &
    col("latency_ms").isNotNull() &
    col("locality").isNotNull() &
    col("event_timestamp").isNotNull()
)
log_dq_check("signal_metrics", "Null removal", df_signal_clean.count(), null_before, total_signal)

# Range validation: signal strength between -140 and -40 dBm
range_fail = df_signal_clean.filter(
    (col("signal_strength_dbm") < -140) | (col("signal_strength_dbm") > -40)
).count()
df_signal_clean = df_signal_clean.filter(
    (col("signal_strength_dbm") >= -140) & (col("signal_strength_dbm") <= -40)
)
log_dq_check("signal_metrics", "Signal range (-140 to -40 dBm)", df_signal_clean.count(), range_fail, total_signal)

# Latency must be positive
latency_fail = df_signal_clean.filter(col("latency_ms") <= 0).count()
df_signal_clean = df_signal_clean.filter(col("latency_ms") > 0)
log_dq_check("signal_metrics", "Latency > 0", df_signal_clean.count(), latency_fail, total_signal)

# Throughput must be non-negative
throughput_fail = df_signal_clean.filter(col("throughput_mbps") < 0).count()
df_signal_clean = df_signal_clean.filter(col("throughput_mbps") >= 0)
log_dq_check("signal_metrics", "Throughput >= 0", df_signal_clean.count(), throughput_fail, total_signal)

# Remove duplicates
before_dedup = df_signal_clean.count()
df_signal_clean = df_signal_clean.dropDuplicates(["event_timestamp", "locality", "signal_strength_dbm"])
log_dq_check("signal_metrics", "Deduplication", df_signal_clean.count(), before_dedup - df_signal_clean.count(), before_dedup)

# Select final silver columns
df_signal_silver = df_signal_clean.select(
    "event_timestamp", "event_date", "event_hour", "event_month", "event_year",
    "locality", "latitude", "longitude",
    "signal_strength_dbm", "signal_quality_pct", "throughput_mbps", "latency_ms",
    "network_type", "network_type_clean", "signal_category", "signal_score",
    "bb60c_dbm", "srsran_dbm", "bladerfxa9_dbm",
    "_ingestion_timestamp", "_batch_id"
).withColumn("_silver_timestamp", current_timestamp())

print(f"\n  Final silver_signal_metrics rows: {df_signal_silver.count():,}")

# COMMAND ----------

df_signal_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_date", "network_type_clean") \
    .save(f"{SILVER_PATH}/signal_metrics")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_signal_metrics
    USING DELTA
    LOCATION '{SILVER_PATH}/signal_metrics'
""")
print("✅ silver_signal_metrics written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Network Performance

# COMMAND ----------

df_5g_bronze = spark.table(f"{DATABASE_NAME}.bronze_5g_network")
total_5g = df_5g_bronze.count()
print(f"Bronze 5G rows: {total_5g:,}")

df_net = df_5g_bronze \
    .withColumnRenamed("Timestamp",                "timestamp_raw") \
    .withColumnRenamed("Location",                 "location") \
    .withColumnRenamed("Signal Strength (dBm)",    "signal_strength_dbm") \
    .withColumnRenamed("Download Speed (Mbps)",    "download_mbps") \
    .withColumnRenamed("Upload Speed (Mbps)",      "upload_mbps") \
    .withColumnRenamed("Latency (ms)",             "latency_ms") \
    .withColumnRenamed("Jitter (ms)",              "jitter_ms") \
    .withColumnRenamed("Network Type",             "network_type") \
    .withColumnRenamed("Device Model",             "device_model") \
    .withColumnRenamed("Carrier",                  "carrier") \
    .withColumnRenamed("Band",                     "band") \
    .withColumnRenamed("Battery Level (%)",        "battery_pct") \
    .withColumnRenamed("Temperature (°C)",         "temperature_c") \
    .withColumnRenamed("Connected Duration (min)", "connected_duration_min") \
    .withColumnRenamed("Handover Count",           "handover_count") \
    .withColumnRenamed("Data Usage (MB)",          "data_usage_mb") \
    .withColumnRenamed("Video Streaming Quality",  "video_quality_score") \
    .withColumnRenamed("VoNR Enabled",             "vonr_enabled") \
    .withColumnRenamed("Network Congestion Level", "congestion_level") \
    .withColumnRenamed("Ping to Google (ms)",      "ping_google_ms") \
    .withColumnRenamed("Dropped Connection",       "dropped_connection_raw")

# Parse types
df_net = df_net \
    .withColumn("event_timestamp", to_timestamp(col("timestamp_raw"), "yyyy-MM-dd HH:mm:ss.SSSSSS")) \
    .withColumn("event_date",       date_format("event_timestamp", "yyyy-MM-dd")) \
    .withColumn("event_hour",       hour("event_timestamp")) \
    .withColumn("dropped_connection", col("dropped_connection_raw").cast(BooleanType())) \
    .withColumn("vonr_enabled_bool",  col("vonr_enabled").cast(BooleanType())) \
    .withColumn("total_throughput_mbps", col("download_mbps") + col("upload_mbps")) \
    .withColumn(
        "network_quality_index",
        spark_round(
            (
                (100 - col("latency_ms")) * 0.3 +
                (col("download_mbps") / 10.0) * 0.4 +
                (10 - col("jitter_ms")) * 0.3
            ).cast(DoubleType()),
            2
        )
    ) \
    .withColumn(
        "performance_tier",
        when(col("download_mbps") >= 500, "Ultra")
        .when(col("download_mbps") >= 100, "High")
        .when(col("download_mbps") >= 25,  "Medium")
        .otherwise("Low")
    )

# DQ Checks
print("\n📋 Data Quality Checks — Network Performance:")

df_net_clean = df_net.filter(
    col("latency_ms").isNotNull() &
    col("download_mbps").isNotNull() &
    col("location").isNotNull() &
    col("event_timestamp").isNotNull()
)
log_dq_check("network_performance", "Null removal", df_net_clean.count(), total_5g - df_net_clean.count(), total_5g)

df_net_clean = df_net_clean.filter((col("latency_ms") > 0) & (col("download_mbps") > 0))
df_net_clean = df_net_clean.dropDuplicates(["event_timestamp", "location", "device_model"])

df_net_silver = df_net_clean.select(
    "event_timestamp", "event_date", "event_hour",
    "location", "network_type", "carrier", "band",
    "signal_strength_dbm", "download_mbps", "upload_mbps",
    "latency_ms", "jitter_ms", "ping_google_ms",
    "dropped_connection", "vonr_enabled_bool",
    "congestion_level", "handover_count",
    "battery_pct", "temperature_c",
    "connected_duration_min", "data_usage_mb",
    "video_quality_score", "total_throughput_mbps",
    "network_quality_index", "performance_tier", "device_model",
    "_ingestion_timestamp", "_batch_id"
).withColumn("_silver_timestamp", current_timestamp())

print(f"  Final silver_network_performance rows: {df_net_silver.count():,}")

# COMMAND ----------

df_net_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_date", "network_type") \
    .save(f"{SILVER_PATH}/network_performance")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_network_performance
    USING DELTA
    LOCATION '{SILVER_PATH}/network_performance'
""")
print("✅ silver_network_performance written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Customer (Churn)

# COMMAND ----------

df_churn_bronze = spark.table(f"{DATABASE_NAME}.bronze_customer_churn")
total_churn = df_churn_bronze.count()
print(f"Bronze customer rows: {total_churn:,}")

df_cust = df_churn_bronze \
    .withColumn("customer_id",       trim(col("customerID"))) \
    .withColumn("gender",            trim(lower(col("gender")))) \
    .withColumn("senior_citizen",    col("SeniorCitizen").cast(BooleanType())) \
    .withColumn("partner",           when(upper(col("Partner")) == "YES", True).otherwise(False)) \
    .withColumn("dependents",        when(upper(col("Dependents")) == "YES", True).otherwise(False)) \
    .withColumn("tenure_months",     col("tenure").cast(IntegerType())) \
    .withColumn("phone_service",     when(upper(col("PhoneService")) == "YES", True).otherwise(False)) \
    .withColumn("internet_service",  trim(col("InternetService"))) \
    .withColumn("contract_type",     trim(col("Contract"))) \
    .withColumn("paperless_billing", when(upper(col("PaperlessBilling")) == "YES", True).otherwise(False)) \
    .withColumn("payment_method",    trim(col("PaymentMethod"))) \
    .withColumn("monthly_charges",   col("MonthlyCharges").cast(DoubleType())) \
    .withColumn("total_charges",     regexp_replace(col("TotalCharges"), " ", "").cast(DoubleType())) \
    .withColumn("churn",             when(upper(col("Churn")) == "YES", True).otherwise(False)) \
    .withColumn("streaming_tv",      when(upper(col("StreamingTV")) == "YES", True).otherwise(False)) \
    .withColumn("streaming_movies",  when(upper(col("StreamingMovies")) == "YES", True).otherwise(False)) \
    .withColumn("tech_support",      when(upper(col("TechSupport")) == "YES", True).otherwise(False)) \
    .withColumn("online_security",   when(upper(col("OnlineSecurity")) == "YES", True).otherwise(False))

# Derived: Customer Lifetime Value = monthly_charges * tenure
df_cust = df_cust \
    .withColumn("clv", spark_round(col("monthly_charges") * col("tenure_months"), 2)) \
    .withColumn(
        "tenure_segment",
        when(col("tenure_months") <= 6,   "New (0-6m)")
        .when(col("tenure_months") <= 24,  "Growing (7-24m)")
        .when(col("tenure_months") <= 48,  "Established (25-48m)")
        .otherwise("Loyal (48m+)")
    ) \
    .withColumn(
        "revenue_segment",
        when(col("monthly_charges") < 30,  "Low (<$30)")
        .when(col("monthly_charges") < 70,  "Mid ($30-70)")
        .otherwise("High ($70+)")
    ) \
    .withColumn("churn_risk_score",
        spark_round(
            (when(col("contract_type") == "Month-to-month", 40).otherwise(10) +
             when(col("tenure_months") <= 6, 30).otherwise(
                 when(col("tenure_months") <= 12, 20).otherwise(5)
             ) +
             when(col("monthly_charges") > 70, 20).otherwise(10) +
             when(col("internet_service") == "Fiber optic", 10).otherwise(0)
            ).cast(DoubleType()),
            1
        )
    )

# DQ Checks
print("\n📋 Data Quality Checks — Customer Churn:")

df_cust_clean = df_cust.filter(col("customer_id").isNotNull())
log_dq_check("customer", "CustomerID not null", df_cust_clean.count(), total_churn - df_cust_clean.count(), total_churn)

df_cust_clean = df_cust_clean.filter(col("monthly_charges").isNotNull() & (col("monthly_charges") > 0))
df_cust_clean = df_cust_clean.dropDuplicates(["customer_id"])

df_cust_silver = df_cust_clean.select(
    "customer_id", "gender", "senior_citizen", "partner", "dependents",
    "tenure_months", "tenure_segment", "phone_service", "internet_service",
    "online_security", "tech_support", "streaming_tv", "streaming_movies",
    "contract_type", "paperless_billing", "payment_method",
    "monthly_charges", "total_charges", "revenue_segment",
    "churn", "clv", "churn_risk_score",
    "_ingestion_timestamp", "_batch_id"
).withColumn("_silver_timestamp", current_timestamp())

print(f"  Final silver_customer rows: {df_cust_silver.count():,}")
churn_rate = df_cust_silver.filter(col("churn") == True).count() / df_cust_silver.count() * 100
print(f"  Overall churn rate: {churn_rate:.2f}%")

# COMMAND ----------

df_cust_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/customer")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_customer
    USING DELTA
    LOCATION '{SILVER_PATH}/customer'
""")
print("✅ silver_customer written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

tables = ["silver_signal_metrics", "silver_network_performance", "silver_customer"]
print("\n📊 Silver Layer — Final Row Counts:")
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.{table}").collect()[0]["cnt"]
    print(f"  {table}: {count:,} rows")
print("\n✅ Silver transformation complete.")
