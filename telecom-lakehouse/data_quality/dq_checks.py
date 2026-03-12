"""
data_quality/dq_checks.py
=========================
Reusable data quality assertion library for the Telecom Lakehouse platform.

Integrates:
- PySpark assertions (native, no extra dependencies)
- Great Expectations-style expectation classes
- DQ result logging to Delta table

Usage:
    from data_quality.dq_checks import TelecomDQSuite
    suite = TelecomDQSuite(spark, database="telecom_lakehouse")
    results = suite.run_all_checks()
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
from dataclasses import dataclass, field
from typing import List, Optional
import json


# ─── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class DQResult:
    table_name:   str
    check_name:   str
    column_name:  Optional[str]
    passed:       int
    failed:       int
    total:        int
    pass_rate:    float
    status:       str        # PASS / WARN / FAIL
    threshold:    float = 0.0
    details:      str = ""

    def to_dict(self):
        return self.__dict__

    def __str__(self):
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[self.status]
        return (f"{icon} [{self.table_name}] {self.check_name}"
                f" — {self.passed:,}/{self.total:,} passed ({self.pass_rate:.1f}%)")


# ─── Core check functions ──────────────────────────────────────────────────────

class DQCheck:
    """Reusable check primitives."""

    @staticmethod
    def not_null(df: DataFrame, column: str, table: str, warn_pct: float = 99.0) -> DQResult:
        total  = df.count()
        failed = df.filter(col(column).isNull()).count()
        passed = total - failed
        rate   = (passed / total * 100) if total > 0 else 0
        status = "PASS" if rate >= 100 else ("WARN" if rate >= warn_pct else "FAIL")
        return DQResult(table, f"not_null({column})", column, passed, failed, total, round(rate, 2), status, warn_pct)

    @staticmethod
    def value_range(df: DataFrame, column: str, min_val: float, max_val: float,
                    table: str, warn_pct: float = 99.0) -> DQResult:
        total  = df.filter(col(column).isNotNull()).count()
        passed = df.filter((col(column) >= min_val) & (col(column) <= max_val)).count()
        failed = total - passed
        rate   = (passed / total * 100) if total > 0 else 0
        status = "PASS" if rate >= 100 else ("WARN" if rate >= warn_pct else "FAIL")
        return DQResult(table, f"range({column},[{min_val},{max_val}])", column,
                        passed, failed, total, round(rate, 2), status, warn_pct)

    @staticmethod
    def positive_value(df: DataFrame, column: str, table: str, warn_pct: float = 99.0) -> DQResult:
        total  = df.filter(col(column).isNotNull()).count()
        passed = df.filter(col(column) > 0).count()
        failed = total - passed
        rate   = (passed / total * 100) if total > 0 else 0
        status = "PASS" if rate >= 100 else ("WARN" if rate >= warn_pct else "FAIL")
        return DQResult(table, f"positive({column})", column, passed, failed, total, round(rate, 2), status, warn_pct)

    @staticmethod
    def no_duplicates(df: DataFrame, key_columns: List[str], table: str) -> DQResult:
        total   = df.count()
        distinct = df.dropDuplicates(key_columns).count()
        failed  = total - distinct
        rate    = (distinct / total * 100) if total > 0 else 0
        status  = "PASS" if failed == 0 else ("WARN" if rate >= 99.0 else "FAIL")
        return DQResult(table, f"no_duplicates({','.join(key_columns)})", None,
                        distinct, failed, total, round(rate, 2), status,
                        details=f"{failed:,} duplicate rows found")

    @staticmethod
    def accepted_values(df: DataFrame, column: str, allowed: List[str], table: str) -> DQResult:
        total  = df.filter(col(column).isNotNull()).count()
        passed = df.filter(col(column).isin(allowed)).count()
        failed = total - passed
        rate   = (passed / total * 100) if total > 0 else 0
        status = "PASS" if rate >= 100 else ("WARN" if rate >= 95.0 else "FAIL")
        return DQResult(table, f"accepted_values({column})", column, passed, failed, total,
                        round(rate, 2), status, details=f"Allowed: {allowed}")


# ─── Telecom-specific DQ Suite ─────────────────────────────────────────────────

class TelecomDQSuite:
    """
    Runs all data quality checks for the Telecom Lakehouse silver layer.
    Logs results to a Delta DQ log table.
    """

    DQ_LOG_PATH = "/FileStore/telecom/dq_log"

    def __init__(self, spark: SparkSession, database: str = "telecom_lakehouse"):
        self.spark    = spark
        self.database = database
        self.results: List[DQResult] = []

    def _table(self, name: str) -> DataFrame:
        return self.spark.table(f"{self.database}.{name}")

    def check_signal_metrics(self) -> List[DQResult]:
        """DQ checks for silver_signal_metrics."""
        df = self._table("silver_signal_metrics")
        checks = [
            DQCheck.not_null(df, "locality",           "silver_signal_metrics"),
            DQCheck.not_null(df, "event_timestamp",    "silver_signal_metrics"),
            DQCheck.not_null(df, "signal_strength_dbm","silver_signal_metrics"),
            DQCheck.not_null(df, "latency_ms",         "silver_signal_metrics"),
            DQCheck.value_range(df, "signal_strength_dbm", -140, -40, "silver_signal_metrics"),
            DQCheck.positive_value(df, "latency_ms",   "silver_signal_metrics"),
            DQCheck.positive_value(df, "throughput_mbps", "silver_signal_metrics"),
            DQCheck.accepted_values(df, "signal_category",
                                    ["Excellent", "Good", "Fair", "Poor"],
                                    "silver_signal_metrics"),
            DQCheck.no_duplicates(df, ["event_timestamp", "locality"], "silver_signal_metrics"),
        ]
        return checks

    def check_network_performance(self) -> List[DQResult]:
        """DQ checks for silver_network_performance."""
        df = self._table("silver_network_performance")
        checks = [
            DQCheck.not_null(df, "location",       "silver_network_performance"),
            DQCheck.not_null(df, "event_timestamp", "silver_network_performance"),
            DQCheck.not_null(df, "download_mbps",  "silver_network_performance"),
            DQCheck.not_null(df, "latency_ms",     "silver_network_performance"),
            DQCheck.positive_value(df, "download_mbps", "silver_network_performance"),
            DQCheck.positive_value(df, "upload_mbps",   "silver_network_performance"),
            DQCheck.positive_value(df, "latency_ms",    "silver_network_performance"),
            DQCheck.value_range(df, "latency_ms", 0, 2000, "silver_network_performance"),
            DQCheck.value_range(df, "jitter_ms",  0, 1000, "silver_network_performance"),
            DQCheck.accepted_values(df, "network_type",
                                    ["5G", "5G NSA", "5G SA", "4G", "LTE", "3G"],
                                    "silver_network_performance"),
            DQCheck.accepted_values(df, "performance_tier",
                                    ["Ultra", "High", "Medium", "Low"],
                                    "silver_network_performance"),
            DQCheck.no_duplicates(df, ["event_timestamp", "location", "device_model"],
                                  "silver_network_performance"),
        ]
        return checks

    def check_customer(self) -> List[DQResult]:
        """DQ checks for silver_customer."""
        df = self._table("silver_customer")
        checks = [
            DQCheck.not_null(df, "customer_id",      "silver_customer"),
            DQCheck.not_null(df, "monthly_charges",  "silver_customer"),
            DQCheck.not_null(df, "contract_type",    "silver_customer"),
            DQCheck.positive_value(df, "monthly_charges", "silver_customer"),
            DQCheck.positive_value(df, "tenure_months",   "silver_customer"),
            DQCheck.value_range(df, "monthly_charges", 0, 500, "silver_customer"),
            DQCheck.accepted_values(df, "gender",
                                    ["male", "female", "unknown"],
                                    "silver_customer"),
            DQCheck.accepted_values(df, "contract_type",
                                    ["Month-to-month", "One year", "Two year"],
                                    "silver_customer"),
            DQCheck.accepted_values(df, "internet_service",
                                    ["DSL", "Fiber optic", "No"],
                                    "silver_customer"),
            DQCheck.no_duplicates(df, ["customer_id"], "silver_customer"),
        ]
        return checks

    def run_all_checks(self) -> List[DQResult]:
        """Execute all DQ suites and return aggregated results."""
        print("=" * 70)
        print("  📋 TELECOM LAKEHOUSE — DATA QUALITY REPORT")
        print("=" * 70)

        all_results = []
        suites = [
            ("Signal Metrics",      self.check_signal_metrics),
            ("Network Performance", self.check_network_performance),
            ("Customer",            self.check_customer),
        ]

        for suite_name, check_fn in suites:
            print(f"\n  ── {suite_name} ──────────────────────────────────")
            results = check_fn()
            for r in results:
                print(f"  {r}")
            all_results.extend(results)

        # Summary
        total_checks = len(all_results)
        passed_checks = sum(1 for r in all_results if r.status == "PASS")
        failed_checks = sum(1 for r in all_results if r.status == "FAIL")
        warned_checks = sum(1 for r in all_results if r.status == "WARN")

        print(f"\n{'=' * 70}")
        print(f"  SUMMARY: {passed_checks}/{total_checks} checks PASSED | "
              f"{warned_checks} WARN | {failed_checks} FAIL")
        print(f"{'=' * 70}\n")

        self.results = all_results
        self._write_dq_log(all_results)
        return all_results

    def _write_dq_log(self, results: List[DQResult]):
        """Persist DQ results to Delta log table for trend tracking."""
        rows = [(
            r.table_name, r.check_name, r.column_name or "",
            float(r.passed), float(r.failed), float(r.total),
            float(r.pass_rate), r.status, r.details
        ) for r in results]

        schema = StructType([
            StructField("table_name",  StringType(),  False),
            StructField("check_name",  StringType(),  False),
            StructField("column_name", StringType(),  True),
            StructField("passed",      DoubleType(),  False),
            StructField("failed",      DoubleType(),  False),
            StructField("total",       DoubleType(),  False),
            StructField("pass_rate",   DoubleType(),  False),
            StructField("status",      StringType(),  False),
            StructField("details",     StringType(),  True),
        ])

        df_log = self.spark.createDataFrame(rows, schema) \
            .withColumn("run_timestamp", current_timestamp())

        df_log.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self.DQ_LOG_PATH)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.dq_log
            USING DELTA
            LOCATION '{self.DQ_LOG_PATH}'
        """)
        print(f"  ✅ DQ results logged to {self.database}.dq_log")


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # When run as a Databricks job or notebook
    spark = SparkSession.builder.getOrCreate()
    suite = TelecomDQSuite(spark, database="telecom_lakehouse")
    results = suite.run_all_checks()

    # Fail the job if any FAIL-status check exists
    failures = [r for r in results if r.status == "FAIL"]
    if failures:
        raise RuntimeError(
            f"DQ gate failed: {len(failures)} critical checks did not pass. "
            f"Failed checks: {[r.check_name for r in failures]}"
        )
    print("✅ All critical DQ gates passed.")
