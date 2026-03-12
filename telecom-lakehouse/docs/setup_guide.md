# Setup Guide — Telecom Lakehouse on Databricks

## Prerequisites

| Requirement | Version | Notes |
|------------|---------|-------|
| Databricks Runtime | 14.3 LTS+ | Delta Lake 3.x included |
| Python | 3.10+ | Available on DBR 14.3 |
| Cluster type | Standard or Job compute | Community Edition works for dev |
| DBFS access | Required | For reading/writing Delta tables |

---

## Step 1: Repository Setup

```bash
# Clone to your local machine
git clone https://github.com/YOUR_USERNAME/telecom-lakehouse.git
cd telecom-lakehouse

# Install dependencies for local development with Databricks Connect
pip install -r requirements.txt
```

---

## Step 2: Upload Data to DBFS

In Databricks workspace:
1. Go to **Data** → **DBFS** → **FileStore**
2. Create folder: `/FileStore/telecom/raw/`
3. Upload all three CSV files from `data/raw/`

Or via Databricks CLI:
```bash
databricks fs mkdirs dbfs:/FileStore/telecom/raw
databricks fs cp data/raw/signal_metrics.csv       dbfs:/FileStore/telecom/raw/
databricks fs cp data/raw/5g_network_data.csv      dbfs:/FileStore/telecom/raw/
databricks fs cp data/raw/telco_customer_churn.csv dbfs:/FileStore/telecom/raw/
```

---

## Step 3: Import Notebooks

1. In Databricks workspace, go to **Workspace** → **Import**
2. Import notebooks in order from the `notebooks/` directory:
   - `bronze/01_bronze_ingestion.py`
   - `silver/02_silver_transformation.py`
   - `gold/03_gold_aggregation.py`
   - `streaming/04_streaming_pipeline.py`
   - `delta_features/05_delta_features.py`

---

## Step 4: Create Cluster

Recommended cluster configuration:
```
Runtime: 14.3 LTS (Scala 2.12, Spark 3.5.0)
Worker type: i3.xlarge (or Standard_DS3_v2 on Azure)
Workers: 2-4 (autoscale)
Libraries: delta-spark==3.1.0 (installed via PyPI)
```

---

## Step 5: Run Pipelines in Order

Run notebooks sequentially:

1. **`01_bronze_ingestion.py`** — Creates `bronze_*` tables
2. **`02_silver_transformation.py`** — Creates `silver_*` tables
3. **`03_gold_aggregation.py`** — Creates `gold_*` tables
4. **`05_delta_features.py`** — Runs OPTIMIZE, Z-ORDER, VACUUM

---

## Step 6: Configure Automated Job

1. Go to **Workflows** → **Create Job**
2. Import `automation/job_definition.json` via the Jobs API or paste manually
3. Update notebook paths to match your workspace
4. Enable the schedule (every 30 minutes)

Via Databricks CLI:
```bash
databricks jobs create --json @automation/job_definition.json
```

---

## Step 7: Create Dashboards

1. Go to **SQL** → **Dashboards** → **Create Dashboard**
2. Create a new dashboard for each:
   - "Network Performance" — use queries from `dashboards/queries/network_performance.sql`
   - "Signal Quality" — use queries from `dashboards/queries/signal_quality_and_customer.sql`
   - "Customer Experience" — use queries from `dashboards/queries/signal_quality_and_customer.sql`
3. Add widgets (Line chart, Bar chart, Counter, Scatter) per query
4. Set auto-refresh to 30 minutes

---

## Step 8: Run Streaming Demo

1. Open `04_streaming_pipeline.py`
2. Run cells 1–4 to start the rate-source simulation
3. Observe bronze and silver tables receiving real-time rows
4. Stop streams using the final cell when done

---

## DBFS Folder Structure After Setup

```
/FileStore/telecom/
├── raw/                    # Source CSV files
├── bronze/                 # Bronze Delta tables
│   ├── signal_metrics/
│   ├── 5g_network/
│   ├── customer_churn/
│   └── stream_network/     # Streaming bronze
├── silver/                 # Silver Delta tables
│   ├── signal_metrics/
│   ├── network_performance/
│   ├── customer/
│   └── stream_network/
├── gold/                   # Gold Delta tables
│   ├── network_kpis/
│   ├── signal_quality/
│   ├── customer_experience/
│   └── churn_analysis/
├── dq_log/                 # DQ audit log
└── checkpoints/            # Streaming checkpoints
```
