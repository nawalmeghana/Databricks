# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Delta Lake Features — Time Travel, Merge, Z-Order, Optimize
# MAGIC
# MAGIC Demonstrates production Delta Lake capabilities used in this platform.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("TelecomLakehouse-DeltaFeatures") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

DATABASE_NAME = "telecom_lakehouse"
SILVER_PATH   = "/FileStore/telecom/silver"
spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Time Travel — Query Historical Versions

# COMMAND ----------

# View the full version history of a table
spark.sql(f"DESCRIBE HISTORY {DATABASE_NAME}.silver_customer").show(10, truncate=False)

# COMMAND ----------

# Query data AS OF a specific version (useful for debugging and reprocessing)
df_v0 = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load(f"{SILVER_PATH}/customer")

print(f"Version 0 row count: {df_v0.count():,}")

# Query AS OF a timestamp
# df_yesterday = spark.read \
#     .format("delta") \
#     .option("timestampAsOf", "2024-01-01") \
#     .load(f"{SILVER_PATH}/customer")

# COMMAND ----------

# SQL time travel syntax
spark.sql(f"""
    SELECT COUNT(*) AS row_count, 'version_0' AS snapshot
    FROM {DATABASE_NAME}.silver_customer VERSION AS OF 0
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MERGE (Upsert) — Incremental Updates to Silver Customer

# COMMAND ----------

# Simulate incoming updated customer records (e.g., contract changes, churn updates)
updated_customers = spark.createDataFrame([
    ("7590-VHVEG", "One year",          29.85, False, "Updated contract"),
    ("5575-GNVDE", "Month-to-month",    65.00, True,  "Churned this month"),
    ("NEW-CUST-01", "Month-to-month",   45.00, False, "New customer"),
], ["customer_id", "contract_type", "monthly_charges", "churn", "_update_source"])

# Load target Delta table
delta_customer = DeltaTable.forPath(spark, f"{SILVER_PATH}/customer")

# Perform MERGE: update existing, insert new
delta_customer.alias("target").merge(
    updated_customers.alias("updates"),
    "target.customer_id = updates.customer_id"
).whenMatchedUpdate(set={
    "contract_type":    "updates.contract_type",
    "monthly_charges":  "updates.monthly_charges",
    "churn":            "updates.churn",
    "_silver_timestamp": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "customer_id":      "updates.customer_id",
    "contract_type":    "updates.contract_type",
    "monthly_charges":  "updates.monthly_charges",
    "churn":            "updates.churn",
    "gender":           "lit('unknown')",
    "_silver_timestamp": "current_timestamp()"
}).execute()

print("✅ MERGE complete — customer table updated with incremental records")

# Verify the upsert
spark.sql(f"""
    SELECT customer_id, contract_type, monthly_charges, churn, _silver_timestamp
    FROM {DATABASE_NAME}.silver_customer
    WHERE customer_id IN ('7590-VHVEG', '5575-GNVDE', 'NEW-CUST-01')
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema Evolution — Add New Column Without Full Rewrite

# COMMAND ----------

# Add a new "nps_score" (Net Promoter Score) column to silver_network_performance
# In production this comes from a new data feed

spark.sql(f"""
    ALTER TABLE {DATABASE_NAME}.silver_network_performance
    ADD COLUMNS (nps_score DOUBLE COMMENT 'Net Promoter Score from customer surveys')
""")

print("✅ Schema evolved — nps_score column added to silver_network_performance")
spark.sql(f"DESCRIBE {DATABASE_NAME}.silver_network_performance").filter(
    col("col_name") == "nps_score"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. OPTIMIZE — File Compaction

# COMMAND ----------

# Delta accumulates many small files from streaming and incremental writes.
# OPTIMIZE merges them into larger, efficient Parquet files.

spark.sql(f"OPTIMIZE {DATABASE_NAME}.silver_network_performance")
print("✅ OPTIMIZE complete — small files compacted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Z-ORDER — Columnar Co-location for Faster Queries

# COMMAND ----------

# Z-ORDER physically co-locates rows with similar values for the specified columns.
# This dramatically speeds up queries that filter on these columns.

# For network performance: most dashboard queries filter by location and event_date
spark.sql(f"""
    OPTIMIZE {DATABASE_NAME}.silver_network_performance
    ZORDER BY (location, event_date, network_type)
""")
print("✅ Z-ORDER complete on silver_network_performance (location, event_date, network_type)")

# For gold network KPIs
spark.sql(f"""
    OPTIMIZE {DATABASE_NAME}.gold_network_kpis
    ZORDER BY (location, event_date, carrier)
""")
print("✅ Z-ORDER complete on gold_network_kpis (location, event_date, carrier)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. VACUUM — Remove Old Files to Save Storage

# COMMAND ----------

# Remove files older than 7 days (168 hours) that are no longer referenced by Delta log
# Default retention is 7 days; do NOT reduce below that in production

spark.sql(f"VACUUM {DATABASE_NAME}.silver_network_performance RETAIN 168 HOURS")
print("✅ VACUUM complete — unreferenced files older than 7 days removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Table Properties — Performance Configuration

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {DATABASE_NAME}.gold_network_kpis
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.dataSkippingNumIndexedCols' = '5',
        'comment' = 'Gold layer network KPIs - auto-optimized'
    )
""")
print("✅ Table properties set for auto-optimization on gold_network_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta Statistics & Data Skipping

# COMMAND ----------

# Delta automatically collects min/max statistics for data skipping.
# ANALYZE TABLE re-computes these for accurate query optimization.
spark.sql(f"ANALYZE TABLE {DATABASE_NAME}.gold_network_kpis COMPUTE STATISTICS FOR ALL COLUMNS")
print("✅ Statistics computed for gold_network_kpis")

# Show table detail including file count and size
spark.sql(f"DESCRIBE DETAIL {DATABASE_NAME}.gold_network_kpis").show(vertical=True)
