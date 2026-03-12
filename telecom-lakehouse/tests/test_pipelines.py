"""
tests/test_pipelines.py
=======================
Unit tests for the Telecom Lakehouse pipeline logic.
Run with: pytest tests/ -v

Uses PySpark local mode — no Databricks connection required.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .appName("TelecomLakehouse-Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


# ─── Signal Metrics Tests ──────────────────────────────────────────────────────

class TestSignalMetricsTransformation:

    def test_signal_category_excellent(self, spark):
        """Signal >= -80 dBm should be classified as Excellent."""
        data = [(-75.0,), (-80.0,)]
        df = spark.createDataFrame(data, ["signal_strength_dbm"])
        df = df.withColumn(
            "signal_category",
            col("signal_strength_dbm").cast("double")
        )
        # Apply classification
        from pyspark.sql.functions import when
        df = df.withColumn(
            "signal_category",
            when(col("signal_strength_dbm") >= -80, "Excellent")
            .when(col("signal_strength_dbm") >= -90, "Good")
            .when(col("signal_strength_dbm") >= -100, "Fair")
            .otherwise("Poor")
        )
        results = [r["signal_category"] for r in df.collect()]
        assert all(r == "Excellent" for r in results)

    def test_signal_category_poor(self, spark):
        """Signal < -100 dBm should be classified as Poor."""
        from pyspark.sql.functions import when
        data = [(-105.0,), (-130.0,)]
        df = spark.createDataFrame(data, ["signal_strength_dbm"])
        df = df.withColumn(
            "signal_category",
            when(col("signal_strength_dbm") >= -80, "Excellent")
            .when(col("signal_strength_dbm") >= -90, "Good")
            .when(col("signal_strength_dbm") >= -100, "Fair")
            .otherwise("Poor")
        )
        results = [r["signal_category"] for r in df.collect()]
        assert all(r == "Poor" for r in results)

    def test_invalid_signal_values_removed(self, spark):
        """Values outside -140 to -40 dBm should be filtered out."""
        data = [(-75.0,), (-150.0,), (-30.0,), (-100.0,)]
        df = spark.createDataFrame(data, ["signal_strength_dbm"])
        df_clean = df.filter(
            (col("signal_strength_dbm") >= -140) & (col("signal_strength_dbm") <= -40)
        )
        assert df_clean.count() == 2

    def test_null_removal(self, spark):
        """Null signal values should be removed."""
        data = [(-75.0,), (None,), (-90.0,), (None,)]
        df = spark.createDataFrame(data, ["signal_strength_dbm"])
        df_clean = df.filter(col("signal_strength_dbm").isNotNull())
        assert df_clean.count() == 2

    def test_signal_score_range(self, spark):
        """Signal score should be in 0–100 range for valid inputs."""
        from pyspark.sql.functions import round as spark_round
        data = [(-140.0,), (-90.0,), (-40.0,)]
        df = spark.createDataFrame(data, ["signal_strength_dbm"])
        df = df.withColumn(
            "signal_score",
            spark_round(((col("signal_strength_dbm") + 140) / 100.0 * 100), 2)
        )
        scores = [r["signal_score"] for r in df.collect()]
        assert all(0 <= s <= 100 for s in scores)


# ─── Network Performance Tests ────────────────────────────────────────────────

class TestNetworkPerformanceTransformation:

    def test_performance_tier_ultra(self, spark):
        """Download >= 500 Mbps should be Ultra tier."""
        from pyspark.sql.functions import when
        data = [(600.0,), (500.0,)]
        df = spark.createDataFrame(data, ["download_mbps"])
        df = df.withColumn(
            "performance_tier",
            when(col("download_mbps") >= 500, "Ultra")
            .when(col("download_mbps") >= 100, "High")
            .when(col("download_mbps") >= 25, "Medium")
            .otherwise("Low")
        )
        results = [r["performance_tier"] for r in df.collect()]
        assert all(r == "Ultra" for r in results)

    def test_zero_latency_removed(self, spark):
        """Zero or negative latency values should be removed."""
        data = [(10.0,), (0.0,), (-5.0,), (50.0,)]
        df = spark.createDataFrame(data, ["latency_ms"])
        df_clean = df.filter(col("latency_ms") > 0)
        assert df_clean.count() == 2

    def test_dropped_connection_boolean_cast(self, spark):
        """'True'/'False' strings should cast to boolean correctly."""
        data = [("True",), ("False",), ("True",)]
        df = spark.createDataFrame(data, ["dropped_connection_raw"])
        df = df.withColumn("dropped_connection", col("dropped_connection_raw").cast(BooleanType()))
        results = [r["dropped_connection"] for r in df.collect()]
        assert results == [True, False, True]

    def test_deduplication(self, spark):
        """Duplicate rows on (timestamp, location, device_model) should be removed."""
        data = [
            ("2024-01-01 10:00:00", "NY", "iPhone14"),
            ("2024-01-01 10:00:00", "NY", "iPhone14"),  # duplicate
            ("2024-01-01 11:00:00", "NY", "iPhone14"),
        ]
        df = spark.createDataFrame(data, ["event_timestamp", "location", "device_model"])
        df_dedup = df.dropDuplicates(["event_timestamp", "location", "device_model"])
        assert df_dedup.count() == 2


# ─── Customer Churn Tests ─────────────────────────────────────────────────────

class TestCustomerTransformation:

    def test_churn_yes_maps_to_true(self, spark):
        """'Yes' churn should map to True."""
        from pyspark.sql.functions import upper, when
        data = [("Yes",), ("No",), ("Yes",)]
        df = spark.createDataFrame(data, ["Churn"])
        df = df.withColumn("churn", when(upper(col("Churn")) == "YES", True).otherwise(False))
        results = [r["churn"] for r in df.collect()]
        assert results == [True, False, True]

    def test_customer_id_not_null(self, spark):
        """Rows with null customer_id must be removed."""
        data = [("CUST-001",), (None,), ("CUST-002",)]
        df = spark.createDataFrame(data, ["customer_id"])
        df_clean = df.filter(col("customer_id").isNotNull())
        assert df_clean.count() == 2

    def test_clv_calculation(self, spark):
        """CLV = monthly_charges × tenure_months."""
        from pyspark.sql.functions import round as spark_round
        data = [(50.0, 12), (100.0, 6), (25.0, 24)]
        df = spark.createDataFrame(data, ["monthly_charges", "tenure_months"])
        df = df.withColumn("clv", spark_round(col("monthly_charges") * col("tenure_months"), 2))
        results = [r["clv"] for r in df.collect()]
        assert results == [600.0, 600.0, 600.0]

    def test_total_charges_space_removal(self, spark):
        """TotalCharges with spaces should be coerced to double."""
        from pyspark.sql.functions import regexp_replace
        data = [("1234.56",), (" ",), ("789.00",)]
        df = spark.createDataFrame(data, ["TotalCharges"])
        df = df.withColumn(
            "total_charges",
            regexp_replace(col("TotalCharges"), " ", "").cast(DoubleType())
        )
        results = [r["total_charges"] for r in df.collect()]
        assert results[0] == 1234.56
        assert results[1] is None   # " " → "" → null on cast
        assert results[2] == 789.00

    def test_tenure_segment_new(self, spark):
        """Tenure ≤ 6 months should be 'New (0-6m)'."""
        from pyspark.sql.functions import when
        data = [(3,), (6,), (7,)]
        df = spark.createDataFrame(data, ["tenure_months"])
        df = df.withColumn(
            "tenure_segment",
            when(col("tenure_months") <= 6, "New (0-6m)")
            .when(col("tenure_months") <= 24, "Growing (7-24m)")
            .otherwise("Other")
        )
        results = [r["tenure_segment"] for r in df.collect()]
        assert results[0] == "New (0-6m)"
        assert results[1] == "New (0-6m)"
        assert results[2] == "Growing (7-24m)"


# ─── Data Quality Check Tests ─────────────────────────────────────────────────

class TestDataQualityChecks:

    def test_not_null_check(self, spark):
        """DQ not-null check should detect nulls correctly."""
        data = [("a",), (None,), ("c",)]
        df = spark.createDataFrame(data, ["col_a"])
        failed = df.filter(col("col_a").isNull()).count()
        assert failed == 1

    def test_range_check(self, spark):
        """Range check should flag out-of-bound values."""
        data = [(-75.0,), (-150.0,), (-90.0,), (-35.0,)]
        df = spark.createDataFrame(data, ["signal"])
        failed = df.filter(~((col("signal") >= -140) & (col("signal") <= -40))).count()
        assert failed == 2

    def test_accepted_values_check(self, spark):
        """Accepted values check should detect invalid categories."""
        data = [("5G",), ("4G",), ("WiFi",), ("LTE",)]
        df = spark.createDataFrame(data, ["network_type"])
        allowed = ["5G", "5G NSA", "4G", "LTE", "3G"]
        failed = df.filter(~col("network_type").isin(allowed)).count()
        assert failed == 1  # "WiFi" is invalid
