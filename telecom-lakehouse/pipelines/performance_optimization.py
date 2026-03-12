# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Performance Optimization Guide
# MAGIC
# MAGIC Techniques used in the Telecom Lakehouse to ensure fast query performance
# MAGIC at scale.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TelecomLakehouse-Optimization") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

DATABASE_NAME = "telecom_lakehouse"
spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Partitioning Strategy
# MAGIC
# MAGIC **Why it matters:** Spark reads only partitions matching the WHERE clause (partition pruning).
# MAGIC Without partitioning, every query scans ALL files.
# MAGIC
# MAGIC | Table | Partition Columns | Rationale |
# MAGIC |-------|------------------|-----------|
# MAGIC | silver_signal_metrics | event_date, network_type_clean | Most queries filter by date + network |
# MAGIC | silver_network_performance | event_date, network_type | Same — dashboards are date-ranged |
# MAGIC | gold_network_kpis | event_date, network_type | Dashboard queries always filter by date |
# MAGIC | gold_customer_experience | contract_type | Churn analysis slices by contract |
# MAGIC
# MAGIC **Rule of thumb:** Partition on columns with 10–1000 distinct values used in WHERE clauses.
# MAGIC Never partition on high-cardinality columns (customer_id, event_id).

# COMMAND ----------

# Demonstrate partition pruning (check query plan for "PartitionFilters")
df = spark.sql(f"""
    EXPLAIN FORMATTED
    SELECT location, AVG(avg_latency_ms)
    FROM {DATABASE_NAME}.gold_network_kpis
    WHERE event_date = '2025-05-28'
    AND   network_type = '5G'
    GROUP BY location
""")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Z-Ordering — Within-Partition Co-location
# MAGIC
# MAGIC **Why it matters:** After partition pruning, Spark still scans many Parquet files.
# MAGIC Z-ORDER physically co-locates rows with similar values for the specified columns
# MAGIC within each partition. Combined with Delta's file-level min/max statistics,
# MAGIC this enables **data skipping** — Spark skips entire Parquet files that can't
# MAGIC contain the queried values.
# MAGIC
# MAGIC **Impact:** Typically 2–10× faster queries on filtered columns.

# COMMAND ----------

# Z-ORDER all production tables on their most-queried filter columns
zorder_configs = [
    ("gold_network_kpis",        ["location", "carrier"]),
    ("gold_signal_quality",      ["locality", "network_type_clean"]),
    ("gold_churn_analysis",      ["contract_type", "internet_service"]),
    ("silver_network_performance", ["location", "network_type", "carrier"]),
]

for table, zorder_cols in zorder_configs:
    cols_str = ", ".join(zorder_cols)
    print(f"Z-ORDERing {table} on ({cols_str})...")
    spark.sql(f"OPTIMIZE {DATABASE_NAME}.{table} ZORDER BY ({cols_str})")
    print(f"  ✅ Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Caching — Keep Hot Data in Memory
# MAGIC
# MAGIC **When to use:** Dashboard queries that repeatedly scan the same Gold tables.
# MAGIC Caching avoids re-reading from DBFS on every query.

# COMMAND ----------

# Cache the most-queried Gold tables for dashboard performance
spark.sql(f"CACHE TABLE {DATABASE_NAME}.gold_network_kpis")
spark.sql(f"CACHE TABLE {DATABASE_NAME}.gold_churn_analysis")
print("✅ Hot Gold tables cached in memory")

# Verify cache status
spark.sql("SHOW CACHED TABLES").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. File Compaction (OPTIMIZE without Z-ORDER)
# MAGIC
# MAGIC **Why it matters:** Streaming and incremental writes produce many small files.
# MAGIC Small files hurt performance: more files = more S3/ADLS API calls, slower reads.
# MAGIC OPTIMIZE merges small files into ~1GB target-size Parquet files.

# COMMAND ----------

# Compact small files from streaming tables
streaming_tables = ["silver_network_performance", "silver_signal_metrics"]
for table in streaming_tables:
    print(f"Compacting {table}...")
    spark.sql(f"OPTIMIZE {DATABASE_NAME}.{table}")
    print(f"  ✅ Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Spark Configuration Tuning
# MAGIC
# MAGIC These settings are configured at cluster/session level for optimal performance
# MAGIC with Delta Lake on this workload.

# COMMAND ----------

optimized_configs = {
    # Parallelism
    "spark.sql.shuffle.partitions":            "200",       # Default 200; reduce for small data
    "spark.sql.adaptive.enabled":              "true",       # AQE: auto-adjusts plans at runtime
    "spark.sql.adaptive.coalescePartitions.enabled": "true", # AQE: merge tiny shuffle partitions

    # Delta-specific
    "spark.databricks.delta.optimizeWrite.enabled":  "true", # Auto-optimize write file sizes
    "spark.databricks.delta.autoCompact.enabled":    "true", # Auto-compact after writes

    # Broadcast joins for dimension tables (< 10MB)
    "spark.sql.autoBroadcastJoinThreshold":   str(10 * 1024 * 1024),  # 10MB

    # Parquet reading
    "spark.sql.parquet.filterPushdown":        "true",
    "spark.sql.parquet.mergeSchema":           "false",      # Disable for performance (schema stable)
}

for key, value in optimized_configs.items():
    spark.conf.set(key, value)
    print(f"  set {key} = {value}")

print("\n✅ Spark configurations applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance Benchmarking

# COMMAND ----------

import time

def time_query(sql, label):
    start = time.time()
    spark.sql(sql).collect()
    elapsed = time.time() - start
    print(f"  [{label}]: {elapsed:.2f}s")

print("⏱️ Query benchmark:")

# Cold query (no cache)
spark.sql(f"UNCACHE TABLE IF EXISTS {DATABASE_NAME}.gold_network_kpis")
time_query(
    f"SELECT location, AVG(avg_latency_ms) FROM {DATABASE_NAME}.gold_network_kpis GROUP BY location",
    "gold_network_kpis AVG latency — cold"
)

# Warm query (with cache)
spark.sql(f"CACHE TABLE {DATABASE_NAME}.gold_network_kpis")
time_query(
    f"SELECT location, AVG(avg_latency_ms) FROM {DATABASE_NAME}.gold_network_kpis GROUP BY location",
    "gold_network_kpis AVG latency — cached"
)
