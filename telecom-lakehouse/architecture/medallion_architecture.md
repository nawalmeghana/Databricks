# Telecom Lakehouse — Architecture Documentation

## Why Medallion Architecture?

The medallion architecture (Bronze → Silver → Gold) solves the core data engineering challenges
in telecom environments:

| Challenge | Solution |
|-----------|----------|
| Raw data arrives dirty, incomplete | Bronze preserves originals for reprocessing |
| Multiple teams need different data shapes | Silver provides a trusted, clean single source |
| Executives need KPIs, not raw rows | Gold delivers pre-aggregated business metrics |
| Streaming + batch must coexist | Delta Lake handles both with ACID transactions |
| Schema changes from vendors | Schema evolution at each layer independently |

## How Delta Lake Enables This Architecture

Delta Lake adds database-grade reliability to files in cloud storage:

- **ACID Transactions**: Each layer write is atomic — no partial data corruption
- **Schema Enforcement**: Prevents bad schemas from entering any layer
- **Time Travel**: Every table version is queryable; supports reprocessing
- **Upserts (MERGE)**: Silver and Gold layers can incrementally update without full rewrites
- **Optimize + Z-Order**: Columnar file layout for query pruning on location/time columns
- **Streaming + Batch Unification**: The same Delta table can receive batch writes and streaming writes

## Data Flow

```
[CSV Files / Streaming Events]
         │
         ▼
  ┌─────────────┐
  │   BRONZE    │  ← Append-only, raw, timestamped ingestion
  │  Delta Lake │    Schema enforced but no value transformations
  └──────┬──────┘    Partition: ingestion_date
         │
         ▼
  ┌─────────────┐
  │   SILVER    │  ← Cleaned, typed, deduplicated, enriched
  │  Delta Lake │    Null removal, range validation, derived columns
  └──────┬──────┘    Partition: date / location
         │
         ▼
  ┌─────────────┐
  │    GOLD     │  ← Aggregated KPIs, business metrics, analytics-ready
  │  Delta Lake │    Optimized for dashboard query patterns
  └──────┬──────┘    Z-Ordered on most-filtered columns
         │
         ▼
  ┌─────────────────────┐
  │  Dashboards / SQL   │
  │  Databricks SQL     │
  │  BI Tools / APIs    │
  └─────────────────────┘
```

## Layer Responsibilities

### Bronze Layer
- Ingest raw CSV files as-is
- Add metadata: `_ingestion_timestamp`, `_source_file`, `_batch_id`
- Enforce structural schema (column names, basic types)
- Never delete or modify; append-only
- Tables: `bronze_signal_metrics`, `bronze_5g_network`, `bronze_customer_churn`

### Silver Layer
- Remove nulls, duplicates, out-of-range values
- Parse timestamps to proper datetime types
- Normalize column names (snake_case, no spaces)
- Add business-meaningful derived columns (signal quality category, churn flag, etc.)
- Apply data quality checks and log failures
- Tables: `silver_signal_metrics`, `silver_network_performance`, `silver_customer`

### Gold Layer
- Aggregate to KPI granularity (hourly/daily/monthly)
- Join across silver tables to produce unified views
- Store as optimized Delta tables for fast dashboard queries
- Tables: `gold_network_kpis`, `gold_signal_quality`, `gold_customer_experience`, `gold_churn_analysis`
