# Databricks# 📡 Telecom Data Lakehouse Platform
### Production-Grade Data Engineering on Databricks | Delta Lake | Medallion Architecture

---

## 📋 Project Overview

This project implements a **complete, production-grade Data Lakehouse** for a telecommunications company using **Databricks**, **Delta Lake**, and **PySpark**. It ingests three real-world telecom datasets, processes them through a **Bronze → Silver → Gold medallion architecture**, and produces analytics dashboards, KPIs, and ML-ready features.

This is designed to demonstrate senior data engineering skills suitable for portfolio, academic submission, or technical interviews.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TELECOM DATA LAKEHOUSE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  DATA SOURCES          INGESTION           STORAGE LAYERS          │
│  ┌──────────────┐      ┌──────────┐        ┌───────────────────┐  │
│  │signal_metrics│─────▶│          │        │   🥉 BRONZE        │  │
│  │.csv          │      │  PySpark │───────▶│  Raw Delta Tables │  │
│  │              │      │  Batch + │        │  bronze_signal    │  │
│  │5g_network    │─────▶│  Spark   │        │  bronze_5g        │  │
│  │_data.csv     │      │  Struct. │        │  bronze_churn     │  │
│  │              │      │  Stream  │        └────────┬──────────┘  │
│  │telco_customer│─────▶│          │                 │             │
│  │_churn.csv    │      └──────────┘                 ▼             │
│  └──────────────┘                          ┌───────────────────┐  │
│                                            │   🥈 SILVER        │  │
│  STREAMING SIM         DATA QUALITY        │  Cleaned + Typed  │  │
│  ┌──────────────┐      ┌──────────┐        │  silver_signal    │  │
│  │Structured    │      │Great     │───────▶│  silver_network   │  │
│  │Streaming     │      │Expects + │        │  silver_customer  │  │
│  │(rate source) │      │PySpark   │        └────────┬──────────┘  │
│  └──────────────┘      │assertions│                 │             │
│                        └──────────┘                 ▼             │
│                                            ┌───────────────────┐  │
│  ORCHESTRATION         DELTA FEATURES      │   🥇 GOLD          │  │
│  ┌──────────────┐      ┌──────────┐        │  KPIs + Agg.      │  │
│  │Databricks    │      │Time      │───────▶│  gold_network     │  │
│  │Jobs /        │      │Travel    │        │  gold_signal      │  │
│  │Workflows     │      │Merge     │        │  gold_customer    │  │
│  │(30min cron)  │      │Z-Order   │        │  gold_churn       │  │
│  └──────────────┘      └──────────┘        └────────┬──────────┘  │
│                                                     │             │
│                                                     ▼             │
│                                   ┌─────────────────────────────┐ │
│                                   │  📊 ANALYTICS & DASHBOARDS  │ │
│                                   │  Network Performance        │ │
│                                   │  Signal Quality             │ │
│                                   │  Customer Experience        │ │
│                                   │  Churn Analysis             │ │
│                                   └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| Cloud Platform | Databricks (AWS/Azure/GCP) |
| Storage Format | Delta Lake |
| Processing Engine | Apache Spark (PySpark) |
| Query Language | Spark SQL |
| Orchestration | Databricks Workflows / Jobs |
| Streaming | Spark Structured Streaming |
| Data Quality | PySpark Assertions + Great Expectations |
| IDE | VSCode + Databricks Extension |
| Version Control | GitHub |
| Dashboards | Databricks SQL Dashboards |
| Data Modeling | Star Schema |

---

## 📂 Repository Structure

```
telecom-lakehouse/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI
│
├── architecture/
│   ├── architecture_diagram.md        # Full system design
│   ├── medallion_architecture.md      # Layer descriptions
│   └── star_schema.md                 # Data model design
│
├── data/
│   ├── raw/                           # Source CSV files
│   │   ├── signal_metrics.csv
│   │   ├── 5g_network_data.csv
│   │   └── telco_customer_churn.csv
│   └── samples/                       # Sample records for testing
│
├── notebooks/
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py     # Raw data ingestion
│   ├── silver/
│   │   └── 02_silver_transformation.py # Cleaning & transformation
│   ├── gold/
│   │   └── 03_gold_aggregation.py     # KPI computation
│   ├── streaming/
│   │   └── 04_streaming_pipeline.py   # Real-time simulation
│   └── delta_features/
│       └── 05_delta_features.py       # Time travel, merge, Z-order
│
├── pipelines/
│   ├── bronze_pipeline.py
│   ├── silver_pipeline.py
│   └── gold_pipeline.py
│
├── data_quality/
│   └── dq_checks.py                   # All quality assertion logic
│
├── models/
│   ├── star_schema.sql                # DDL for star schema
│   └── data_model.md                  # Model documentation
│
├── dashboards/
│   └── queries/
│       ├── network_performance.sql
│       ├── signal_quality.sql
│       └── customer_experience.sql
│
├── streaming/
│   └── streaming_simulation.py        # Structured Streaming code
│
├── automation/
│   ├── job_definition.json            # Databricks Job config
│   └── workflow_definition.yml        # Workflow YAML
│
├── tests/
│   └── test_pipelines.py              # Unit tests
│
└── docs/
    ├── setup_guide.md
    ├── kpi_definitions.md
    └── business_insights.md
```

---

## 🚀 Quick Start

### Prerequisites
- Databricks workspace (Community Edition or above)
- Python 3.9+
- Git

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/telecom-lakehouse.git
cd telecom-lakehouse

# 2. Install dependencies
pip install -r requirements.txt

# 3. Upload to Databricks
# Import notebooks from /notebooks folder into your Databricks workspace
# Upload CSV files from /data/raw to DBFS: /FileStore/telecom/raw/

# 4. Run pipelines in order
# Run: notebooks/bronze/01_bronze_ingestion.py
# Run: notebooks/silver/02_silver_transformation.py
# Run: notebooks/gold/03_gold_aggregation.py
```

---

## 📊 Datasets

| Dataset | Rows | Description |
|---------|------|-------------|
| signal_metrics.csv | 16,829 | Radio signal telemetry (RSRP, throughput, latency by location) |
| 5g_network_data.csv | 50,000 | 5G/4G network performance (speed, jitter, dropped connections) |
| telco_customer_churn.csv | 7,043 | Customer demographics, services, and churn labels |

---

## 🏅 Key KPIs Implemented

**Network KPIs**
- Average Latency (ms) by location and time
- Throughput Performance (download/upload Mbps)
- Dropped Connection Rate (%)
- Network Quality Index (composite score)
- Jitter Distribution

**Signal KPIs**
- Average Signal Strength (dBm)
- Signal Quality Score (%)
- RSRP / RSRQ / SINR classification
- Signal category (Excellent / Good / Fair / Poor)

**Customer KPIs**
- Churn Rate (%)
- Average Revenue Per User (ARPU)
- Customer Lifetime Value (CLV)
- Retention Rate
- Churn by contract type

---

## 💡 Business Insights

1. **Poor signal quality directly correlates with higher dropped connection rates**
2. **Month-to-month contract customers churn at 3x the rate of annual contract customers**
3. **5G connections deliver 4-8x throughput vs 4G at comparable signal strength**
4. **Customers with <6 months tenure and high monthly charges are highest churn risk**
5. **Network congestion peaks correlate with latency spikes and increased churn propensity**

---

## 👤 Author

Built as a production-grade portfolio project demonstrating senior Data Engineering skills on Databricks Lakehouse architecture.

**Skills demonstrated:** PySpark, Delta Lake, Medallion Architecture, Structured Streaming, Star Schema, Data Quality, Databricks Workflows, SQL Analytics, Telecom Domain Knowledge
