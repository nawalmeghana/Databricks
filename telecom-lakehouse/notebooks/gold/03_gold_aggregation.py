# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — KPI Aggregations & Analytics Tables
# MAGIC
# MAGIC **Purpose:** Build business-ready analytics tables with pre-aggregated KPIs.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `gold_network_kpis`       — Hourly network performance metrics
# MAGIC - `gold_signal_quality`     — Signal quality analytics by location
# MAGIC - `gold_customer_experience`— Customer metrics joined with network data
# MAGIC - `gold_churn_analysis`     — Churn cohort analysis

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, countDistinct,
    max as spark_max, min as spark_min, stddev,
    when, round as spark_round, lit, current_timestamp,
    percentile_approx, first
)
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("TelecomLakehouse-GoldAggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

DATABASE_NAME = "telecom_lakehouse"
GOLD_PATH     = "/FileStore/telecom/gold"
spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. gold_network_kpis — Hourly Network KPIs

# COMMAND ----------

df_net = spark.table(f"{DATABASE_NAME}.silver_network_performance")

df_gold_net = df_net.groupBy(
    "event_date", "event_hour", "location", "network_type", "carrier"
).agg(
    # Throughput KPIs
    spark_round(avg("download_mbps"), 2).alias("avg_download_mbps"),
    spark_round(avg("upload_mbps"),   2).alias("avg_upload_mbps"),
    spark_round(spark_max("download_mbps"), 2).alias("max_download_mbps"),
    spark_round(spark_min("download_mbps"), 2).alias("min_download_mbps"),
    spark_round(stddev("download_mbps"),    2).alias("stddev_download_mbps"),

    # Latency KPIs
    spark_round(avg("latency_ms"),          2).alias("avg_latency_ms"),
    spark_round(percentile_approx("latency_ms", 0.50), 2).alias("p50_latency_ms"),
    spark_round(percentile_approx("latency_ms", 0.95), 2).alias("p95_latency_ms"),
    spark_round(percentile_approx("latency_ms", 0.99), 2).alias("p99_latency_ms"),

    # Jitter KPIs
    spark_round(avg("jitter_ms"), 2).alias("avg_jitter_ms"),

    # Connection KPIs
    count("*").alias("total_sessions"),
    spark_sum(when(col("dropped_connection") == True, 1).otherwise(0)).alias("dropped_connections"),
    spark_round(
        (spark_sum(when(col("dropped_connection") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("dropped_connection_rate_pct"),

    # Data usage
    spark_round(avg("data_usage_mb"),         2).alias("avg_data_usage_mb"),
    spark_round(spark_sum("data_usage_mb"),    2).alias("total_data_usage_mb"),

    # Congestion distribution
    spark_round(avg("network_quality_index"),  2).alias("avg_network_quality_index"),
    spark_round(avg("video_quality_score"),    2).alias("avg_video_quality_score"),

    # VoNR penetration
    spark_round(
        (spark_sum(when(col("vonr_enabled_bool") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("vonr_penetration_pct"),

    countDistinct("device_model").alias("unique_devices")
).withColumn(
    "network_health_score",
    spark_round(
        (100 - col("avg_latency_ms")) * 0.25 +
        (col("avg_download_mbps") / 10.0) * 0.35 +
        (100 - col("dropped_connection_rate_pct") * 10) * 0.20 +
        col("avg_network_quality_index") * 0.20,
        2
    )
).withColumn("_gold_timestamp", current_timestamp())

print(f"gold_network_kpis rows: {df_gold_net.count():,}")

# COMMAND ----------

df_gold_net.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_date", "network_type") \
    .save(f"{GOLD_PATH}/network_kpis")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_network_kpis
    USING DELTA
    LOCATION '{GOLD_PATH}/network_kpis'
""")
print("✅ gold_network_kpis written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. gold_signal_quality — Signal Analytics by Location & Date

# COMMAND ----------

df_sig = spark.table(f"{DATABASE_NAME}.silver_signal_metrics")

df_gold_sig = df_sig.groupBy(
    "event_date", "event_hour", "locality", "network_type_clean"
).agg(
    spark_round(avg("signal_strength_dbm"),  2).alias("avg_signal_strength_dbm"),
    spark_round(spark_max("signal_strength_dbm"), 2).alias("max_signal_strength_dbm"),
    spark_round(spark_min("signal_strength_dbm"), 2).alias("min_signal_strength_dbm"),
    spark_round(avg("signal_quality_pct"),   2).alias("avg_signal_quality_pct"),
    spark_round(avg("throughput_mbps"),      2).alias("avg_throughput_mbps"),
    spark_round(avg("latency_ms"),           2).alias("avg_latency_ms"),
    spark_round(avg("signal_score"),         2).alias("avg_signal_score"),
    count("*").alias("measurement_count"),

    # Signal category distribution
    spark_round(
        (spark_sum(when(col("signal_category") == "Excellent", 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("pct_excellent_signal"),
    spark_round(
        (spark_sum(when(col("signal_category") == "Good", 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("pct_good_signal"),
    spark_round(
        (spark_sum(when(col("signal_category") == "Fair", 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("pct_fair_signal"),
    spark_round(
        (spark_sum(when(col("signal_category") == "Poor", 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("pct_poor_signal"),

    # Instrument comparison
    spark_round(avg("bb60c_dbm"),      2).alias("avg_bb60c_dbm"),
    spark_round(avg("srsran_dbm"),     2).alias("avg_srsran_dbm"),
    spark_round(avg("bladerfxa9_dbm"), 2).alias("avg_bladerfxa9_dbm"),
).withColumn("_gold_timestamp", current_timestamp())

print(f"gold_signal_quality rows: {df_gold_sig.count():,}")

df_gold_sig.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_date", "network_type_clean") \
    .save(f"{GOLD_PATH}/signal_quality")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_signal_quality
    USING DELTA
    LOCATION '{GOLD_PATH}/signal_quality'
""")
print("✅ gold_signal_quality written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. gold_customer_experience — Customer Metrics

# COMMAND ----------

df_cust = spark.table(f"{DATABASE_NAME}.silver_customer")

df_gold_cust = df_cust.groupBy(
    "contract_type", "internet_service", "tenure_segment", "revenue_segment", "gender"
).agg(
    count("*").alias("customer_count"),

    # Revenue KPIs
    spark_round(avg("monthly_charges"),  2).alias("avg_monthly_charges"),  # ARPU
    spark_round(spark_sum("monthly_charges"), 2).alias("total_monthly_revenue"),
    spark_round(avg("total_charges"),    2).alias("avg_total_charges"),
    spark_round(avg("clv"),              2).alias("avg_clv"),
    spark_round(spark_sum("clv"),        2).alias("total_clv"),

    # Churn KPIs
    spark_sum(when(col("churn") == True, 1).otherwise(0)).alias("churned_customers"),
    spark_round(
        (spark_sum(when(col("churn") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("churn_rate_pct"),
    spark_round(100 -
        (spark_sum(when(col("churn") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("retention_rate_pct"),

    # Tenure
    spark_round(avg("tenure_months"),    2).alias("avg_tenure_months"),

    # Services adoption
    spark_round(
        (spark_sum(when(col("streaming_tv") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("pct_streaming_tv"),
    spark_round(
        (spark_sum(when(col("tech_support") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("pct_tech_support"),

    spark_round(avg("churn_risk_score"), 2).alias("avg_churn_risk_score"),
).withColumn("_gold_timestamp", current_timestamp())

print(f"gold_customer_experience rows: {df_gold_cust.count():,}")

df_gold_cust.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("contract_type") \
    .save(f"{GOLD_PATH}/customer_experience")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_customer_experience
    USING DELTA
    LOCATION '{GOLD_PATH}/customer_experience'
""")
print("✅ gold_customer_experience written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. gold_churn_analysis — Churn Cohort Analysis

# COMMAND ----------

df_gold_churn = df_cust.groupBy(
    "contract_type", "internet_service", "payment_method", "tenure_segment"
).agg(
    count("*").alias("total_customers"),
    spark_sum(when(col("churn") == True, 1).otherwise(0)).alias("churned"),
    spark_round(avg("monthly_charges"), 2).alias("avg_monthly_charges"),
    spark_round(avg("tenure_months"),   2).alias("avg_tenure_months"),
    spark_round(avg("clv"),             2).alias("avg_clv"),
    spark_round(
        (spark_sum(when(col("churn") == True, 1).otherwise(0)).cast(DoubleType()) /
         count("*").cast(DoubleType())) * 100,
        2
    ).alias("churn_rate_pct"),
    spark_round(avg("churn_risk_score"), 2).alias("avg_risk_score"),
    spark_sum(when(col("senior_citizen") == True, 1).otherwise(0)).alias("senior_count"),
    spark_sum(when(col("partner") == True, 1).otherwise(0)).alias("with_partner_count"),
    spark_sum(when(col("paperless_billing") == True, 1).otherwise(0)).alias("paperless_count"),
).withColumn(
    "churn_risk_tier",
    when(col("churn_rate_pct") >= 50, "Critical")
    .when(col("churn_rate_pct") >= 30, "High")
    .when(col("churn_rate_pct") >= 15, "Medium")
    .otherwise("Low")
).withColumn("_gold_timestamp", current_timestamp())

print(f"gold_churn_analysis rows: {df_gold_churn.count():,}")

df_gold_churn.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{GOLD_PATH}/churn_analysis")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_churn_analysis
    USING DELTA
    LOCATION '{GOLD_PATH}/churn_analysis'
""")
print("✅ gold_churn_analysis written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

tables = ["gold_network_kpis", "gold_signal_quality", "gold_customer_experience", "gold_churn_analysis"]
print("\n📊 Gold Layer — Final Row Counts:")
for table in tables:
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {DATABASE_NAME}.{table}").collect()[0]["c"]
    print(f"  {table}: {cnt:,} rows")

# Show top-level KPIs
print("\n📈 Key Business KPIs:")
kpis = spark.sql(f"""
    SELECT
        ROUND(AVG(avg_download_mbps), 1)      AS overall_avg_download_mbps,
        ROUND(AVG(avg_latency_ms), 1)          AS overall_avg_latency_ms,
        ROUND(AVG(dropped_connection_rate_pct), 2) AS avg_dropped_conn_pct,
        ROUND(AVG(avg_network_quality_index), 2)   AS avg_nqi
    FROM {DATABASE_NAME}.gold_network_kpis
""")
kpis.show()

churn_kpis = spark.sql(f"""
    SELECT
        ROUND(SUM(churned) * 100.0 / SUM(total_customers), 2) AS overall_churn_rate_pct,
        ROUND(AVG(avg_monthly_charges), 2)                     AS overall_arpu,
        ROUND(AVG(avg_clv), 2)                                 AS overall_avg_clv
    FROM {DATABASE_NAME}.gold_churn_analysis
""")
churn_kpis.show()

print("✅ Gold aggregation complete.")
