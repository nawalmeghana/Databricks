-- ============================================================
-- Star Schema DDL — Telecom Lakehouse Data Model
-- Database: telecom_lakehouse
-- ============================================================

USE telecom_lakehouse;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_time (
    time_key        BIGINT      NOT NULL COMMENT 'Surrogate key: YYYYMMDDHH',
    date_actual     DATE        NOT NULL,
    year            INT         NOT NULL,
    quarter         INT         NOT NULL,
    month           INT         NOT NULL,
    month_name      STRING      NOT NULL,
    week_of_year    INT         NOT NULL,
    day_of_month    INT         NOT NULL,
    day_of_week     INT         NOT NULL,
    day_name        STRING      NOT NULL,
    hour            INT         NOT NULL,
    is_weekend      BOOLEAN     NOT NULL,
    is_peak_hour    BOOLEAN     NOT NULL COMMENT '8am-10pm = peak',
    fiscal_quarter  STRING
)
USING DELTA
COMMENT 'Time dimension — pre-populated for full date range';

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        BIGINT      NOT NULL COMMENT 'Surrogate key',
    customer_id         STRING      NOT NULL COMMENT 'Source system customer ID',
    gender              STRING,
    senior_citizen      BOOLEAN,
    partner             BOOLEAN,
    dependents          BOOLEAN,
    internet_service    STRING,
    streaming_tv        BOOLEAN,
    streaming_movies    BOOLEAN,
    tech_support        BOOLEAN,
    online_security     BOOLEAN,
    phone_service       BOOLEAN,
    multiple_lines      STRING,
    scd_start_date      DATE        COMMENT 'SCD Type 2 start',
    scd_end_date        DATE        COMMENT 'SCD Type 2 end — NULL = current',
    is_current          BOOLEAN     DEFAULT true
)
USING DELTA
COMMENT 'Customer dimension with SCD Type 2 support';

CREATE TABLE IF NOT EXISTS dim_network_cell (
    cell_key        BIGINT      NOT NULL COMMENT 'Surrogate key',
    location        STRING      NOT NULL,
    city            STRING,
    region          STRING,
    country         STRING      DEFAULT 'USA',
    latitude        DOUBLE,
    longitude       DOUBLE,
    network_type    STRING      COMMENT '5G / 4G/LTE / 3G',
    carrier         STRING,
    band            STRING,
    cell_id         STRING
)
USING DELTA
COMMENT 'Network cell / location dimension';

CREATE TABLE IF NOT EXISTS dim_device (
    device_key      BIGINT      NOT NULL COMMENT 'Surrogate key',
    device_model    STRING      NOT NULL,
    manufacturer    STRING,
    os_type         STRING      COMMENT 'iOS / Android',
    vonr_capable    BOOLEAN,
    max_band_5g     STRING
)
USING DELTA
COMMENT 'Device dimension';

-- ============================================================
-- FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_network_performance (
    perf_id             BIGINT      GENERATED ALWAYS AS IDENTITY,
    time_key            BIGINT      NOT NULL REFERENCES dim_time(time_key),
    cell_key            BIGINT      NOT NULL REFERENCES dim_network_cell(cell_key),
    device_key          BIGINT      NOT NULL REFERENCES dim_device(device_key),
    -- Measures
    download_mbps       DOUBLE,
    upload_mbps         DOUBLE,
    latency_ms          DOUBLE,
    jitter_ms           DOUBLE,
    ping_google_ms      DOUBLE,
    dropped_connection  BOOLEAN,
    handover_count      INT,
    data_usage_mb       DOUBLE,
    video_quality_score INT,
    signal_strength_dbm DOUBLE,
    congestion_level    STRING,
    vonr_enabled        BOOLEAN,
    network_quality_index DOUBLE,
    performance_tier    STRING,
    _batch_id           STRING,
    _ingestion_ts       TIMESTAMP
)
USING DELTA
PARTITIONED BY (time_key)
COMMENT 'Fact table — network performance events (grain: one row per session)';

CREATE TABLE IF NOT EXISTS fact_signal_metrics (
    signal_id           BIGINT      GENERATED ALWAYS AS IDENTITY,
    time_key            BIGINT      NOT NULL REFERENCES dim_time(time_key),
    cell_key            BIGINT      NOT NULL REFERENCES dim_network_cell(cell_key),
    -- Measures
    signal_strength_dbm DOUBLE      COMMENT 'RSRP equivalent (dBm)',
    signal_quality_pct  DOUBLE      COMMENT 'RSRQ equivalent (%)',
    throughput_mbps     DOUBLE      COMMENT 'SINR-correlated throughput',
    latency_ms          DOUBLE,
    signal_score        DOUBLE      COMMENT 'Normalized 0-100 composite',
    signal_category     STRING      COMMENT 'Excellent/Good/Fair/Poor',
    bb60c_dbm           DOUBLE,
    srsran_dbm          DOUBLE,
    bladerfxa9_dbm      DOUBLE,
    _batch_id           STRING,
    _ingestion_ts       TIMESTAMP
)
USING DELTA
PARTITIONED BY (time_key)
COMMENT 'Fact table — signal measurement events (grain: one row per measurement)';

CREATE TABLE IF NOT EXISTS fact_customer_usage (
    usage_id            BIGINT      GENERATED ALWAYS AS IDENTITY,
    time_key            BIGINT      NOT NULL REFERENCES dim_time(time_key),
    customer_key        BIGINT      NOT NULL REFERENCES dim_customer(customer_key),
    -- Measures
    monthly_charges     DOUBLE,
    total_charges       DOUBLE,
    tenure_months       INT,
    churn               BOOLEAN,
    churn_risk_score    DOUBLE,
    clv                 DOUBLE      COMMENT 'Customer Lifetime Value',
    contract_type       STRING,
    payment_method      STRING,
    paperless_billing   BOOLEAN,
    revenue_segment     STRING,
    tenure_segment      STRING,
    _batch_id           STRING,
    _ingestion_ts       TIMESTAMP
)
USING DELTA
COMMENT 'Fact table — customer usage snapshot (grain: one row per customer per period)';

-- ============================================================
-- HELPER: Populate dim_time for 2023-2026
-- ============================================================
INSERT INTO dim_time
SELECT
    CAST(DATE_FORMAT(d, 'yyyyMMdd') || LPAD(h, 2, '0') AS BIGINT) AS time_key,
    CAST(d AS DATE)                                                  AS date_actual,
    YEAR(d)         AS year,
    QUARTER(d)      AS quarter,
    MONTH(d)        AS month,
    DATE_FORMAT(d, 'MMMM')                                           AS month_name,
    WEEKOFYEAR(d)   AS week_of_year,
    DAY(d)          AS day_of_month,
    DAYOFWEEK(d)    AS day_of_week,
    DATE_FORMAT(d, 'EEEE')                                           AS day_name,
    h               AS hour,
    DAYOFWEEK(d) IN (1, 7)                                           AS is_weekend,
    h BETWEEN 8 AND 22                                               AS is_peak_hour,
    CONCAT('Q', QUARTER(d), '-', YEAR(d))                           AS fiscal_quarter
FROM (
    SELECT EXPLODE(SEQUENCE(DATE '2023-01-01', DATE '2026-12-31', INTERVAL 1 DAY)) AS d
) dates
CROSS JOIN (
    SELECT EXPLODE(SEQUENCE(0, 23, 1)) AS h
) hours;

-- ============================================================
-- SAMPLE ANALYTICAL QUERIES ON STAR SCHEMA
-- ============================================================

-- Q1: Daily average latency by network type
SELECT
    t.date_actual,
    n.network_type,
    n.carrier,
    ROUND(AVG(f.latency_ms), 2)   AS avg_latency_ms,
    ROUND(AVG(f.download_mbps), 2) AS avg_download_mbps,
    COUNT(*)                       AS session_count
FROM fact_network_performance f
JOIN dim_time         t ON f.time_key = t.time_key
JOIN dim_network_cell n ON f.cell_key  = n.cell_key
WHERE t.year = 2025
GROUP BY t.date_actual, n.network_type, n.carrier
ORDER BY t.date_actual, n.network_type;

-- Q2: Churn rate by contract type and tenure segment
SELECT
    c.internet_service,
    u.contract_type,
    u.tenure_segment,
    COUNT(*)                                          AS customers,
    SUM(CAST(u.churn AS INT))                         AS churned,
    ROUND(AVG(u.monthly_charges), 2)                  AS avg_monthly_charges,
    ROUND(SUM(CAST(u.churn AS INT)) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM fact_customer_usage u
JOIN dim_customer c ON u.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.internet_service, u.contract_type, u.tenure_segment
ORDER BY churn_rate_pct DESC;

-- Q3: Peak hour network performance
SELECT
    t.hour,
    t.day_name,
    t.is_peak_hour,
    ROUND(AVG(f.latency_ms), 2)     AS avg_latency_ms,
    ROUND(AVG(f.download_mbps), 2)  AS avg_download_mbps,
    ROUND(AVG(CAST(f.dropped_connection AS INT)) * 100, 2) AS drop_rate_pct
FROM fact_network_performance f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.hour, t.day_name, t.is_peak_hour
ORDER BY t.hour;
