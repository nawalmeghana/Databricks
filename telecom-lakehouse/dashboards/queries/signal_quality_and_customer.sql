-- ============================================================
-- Dashboard: Signal Quality
-- ============================================================

-- ─── Chart 1: RSRP vs Throughput Scatter (by location) ───────────────────────
SELECT
    locality                    AS location,
    network_type_clean          AS network_type,
    avg_signal_strength_dbm     AS signal_strength_dbm,
    avg_throughput_mbps         AS throughput_mbps,
    avg_latency_ms,
    measurement_count,
    CASE
        WHEN avg_signal_strength_dbm >= -80  THEN 'Excellent'
        WHEN avg_signal_strength_dbm >= -90  THEN 'Good'
        WHEN avg_signal_strength_dbm >= -100 THEN 'Fair'
        ELSE 'Poor'
    END AS signal_category
FROM telecom_lakehouse.gold_signal_quality
ORDER BY avg_signal_strength_dbm DESC;

-- ─── Chart 2: Signal Quality Distribution (Stacked Bar) ─────────────────────
SELECT
    locality            AS location,
    network_type_clean  AS network_type,
    pct_excellent_signal,
    pct_good_signal,
    pct_fair_signal,
    pct_poor_signal,
    avg_signal_score,
    measurement_count
FROM telecom_lakehouse.gold_signal_quality
ORDER BY avg_signal_score DESC;

-- ─── Chart 3: Signal Quality Over Time ───────────────────────────────────────
SELECT
    event_date,
    network_type_clean                              AS network_type,
    ROUND(AVG(avg_signal_strength_dbm), 2)          AS avg_signal_dbm,
    ROUND(AVG(avg_signal_score), 2)                 AS avg_signal_score,
    ROUND(AVG(pct_excellent_signal + pct_good_signal), 2) AS pct_acceptable_signal,
    SUM(measurement_count)                          AS total_measurements
FROM telecom_lakehouse.gold_signal_quality
GROUP BY event_date, network_type_clean
ORDER BY event_date, network_type_clean;

-- ─── Chart 4: Throughput by Signal Category ──────────────────────────────────
SELECT
    CASE
        WHEN avg_signal_strength_dbm >= -80  THEN 'Excellent (>-80dBm)'
        WHEN avg_signal_strength_dbm >= -90  THEN 'Good (-80 to -90dBm)'
        WHEN avg_signal_strength_dbm >= -100 THEN 'Fair (-90 to -100dBm)'
        ELSE 'Poor (<-100dBm)'
    END AS signal_category,
    ROUND(AVG(avg_throughput_mbps), 2)  AS avg_throughput_mbps,
    ROUND(AVG(avg_latency_ms), 2)       AS avg_latency_ms,
    COUNT(*)                             AS location_count,
    SUM(measurement_count)               AS total_samples
FROM telecom_lakehouse.gold_signal_quality
GROUP BY signal_category
ORDER BY avg_throughput_mbps DESC;

-- ─── Chart 5: Instrument Comparison (BB60C vs srsRAN vs BladeRF) ─────────────
SELECT
    locality                    AS location,
    network_type_clean          AS network_type,
    avg_signal_strength_dbm     AS composite_signal,
    avg_bb60c_dbm,
    avg_srsran_dbm,
    avg_bladerfxa9_dbm,
    ABS(avg_bb60c_dbm - avg_srsran_dbm) AS delta_bb60c_srsran
FROM telecom_lakehouse.gold_signal_quality
WHERE avg_bb60c_dbm != 0 AND avg_srsran_dbm != 0
ORDER BY delta_bb60c_srsran DESC;

-- ─── KPI Counter: Signal Quality Summary ─────────────────────────────────────
SELECT
    ROUND(AVG(avg_signal_strength_dbm), 1)              AS fleet_avg_signal_dbm,
    ROUND(AVG(avg_signal_score), 1)                     AS fleet_avg_signal_score,
    ROUND(AVG(pct_excellent_signal + pct_good_signal), 1) AS pct_good_or_better,
    ROUND(AVG(pct_poor_signal), 2)                      AS pct_poor_signal,
    SUM(measurement_count)                              AS total_measurements,
    COUNT(DISTINCT locality)                            AS unique_locations
FROM telecom_lakehouse.gold_signal_quality;


-- ============================================================
-- Dashboard: Customer Experience
-- ============================================================

-- ─── Chart 1: Churn Rate by Contract Type ────────────────────────────────────
SELECT
    contract_type,
    SUM(customer_count)     AS total_customers,
    SUM(churned_customers)  AS churned,
    ROUND(AVG(churn_rate_pct), 2)       AS churn_rate_pct,
    ROUND(AVG(retention_rate_pct), 2)   AS retention_rate_pct,
    ROUND(AVG(avg_monthly_charges), 2)  AS arpu,
    ROUND(AVG(avg_clv), 2)             AS avg_clv
FROM telecom_lakehouse.gold_customer_experience
GROUP BY contract_type
ORDER BY churn_rate_pct DESC;

-- ─── Chart 2: Churn Rate by Monthly Charges Band ─────────────────────────────
SELECT
    revenue_segment,
    SUM(customer_count)                     AS total_customers,
    SUM(churned_customers)                  AS churned,
    ROUND(AVG(churn_rate_pct), 2)           AS churn_rate_pct,
    ROUND(AVG(avg_monthly_charges), 2)      AS avg_monthly_charges,
    ROUND(AVG(avg_clv), 2)                 AS avg_clv,
    ROUND(AVG(avg_churn_risk_score), 2)     AS avg_risk_score
FROM telecom_lakehouse.gold_customer_experience
GROUP BY revenue_segment
ORDER BY avg_monthly_charges;

-- ─── Chart 3: Customer Lifetime Value by Tenure Segment ──────────────────────
SELECT
    tenure_segment,
    contract_type,
    SUM(customer_count)                     AS total_customers,
    ROUND(AVG(avg_clv), 2)                 AS avg_clv,
    ROUND(SUM(total_clv), 2)               AS total_clv,
    ROUND(AVG(avg_monthly_charges), 2)      AS arpu,
    ROUND(AVG(churn_rate_pct), 2)           AS churn_rate_pct
FROM telecom_lakehouse.gold_customer_experience
GROUP BY tenure_segment, contract_type
ORDER BY avg_clv DESC;

-- ─── Chart 4: Internet Service vs Churn ──────────────────────────────────────
SELECT
    internet_service,
    SUM(customer_count)                     AS total_customers,
    SUM(churned_customers)                  AS churned,
    ROUND(AVG(churn_rate_pct), 2)           AS churn_rate_pct,
    ROUND(AVG(avg_monthly_charges), 2)      AS arpu,
    ROUND(AVG(pct_streaming_tv), 2)         AS pct_streaming_tv,
    ROUND(AVG(pct_tech_support), 2)         AS pct_with_tech_support
FROM telecom_lakehouse.gold_customer_experience
GROUP BY internet_service
ORDER BY churn_rate_pct DESC;

-- ─── Chart 5: Churn Risk Tier Distribution ───────────────────────────────────
SELECT
    churn_risk_tier,
    COUNT(*)                                AS cohort_count,
    SUM(total_customers)                    AS total_customers,
    ROUND(AVG(churn_rate_pct), 2)           AS actual_churn_rate_pct,
    ROUND(AVG(avg_monthly_charges), 2)      AS avg_monthly_charges,
    ROUND(SUM(total_customers) * AVG(avg_monthly_charges), 2) AS monthly_revenue_at_risk
FROM telecom_lakehouse.gold_churn_analysis
GROUP BY churn_risk_tier
ORDER BY actual_churn_rate_pct DESC;

-- ─── Chart 6: Revenue at Risk by Cohort ──────────────────────────────────────
SELECT
    contract_type,
    internet_service,
    tenure_segment,
    total_customers,
    churned,
    churn_rate_pct,
    ROUND(total_customers * avg_monthly_charges * (churn_rate_pct / 100.0), 2) AS monthly_revenue_at_risk,
    avg_clv,
    churn_risk_tier
FROM telecom_lakehouse.gold_churn_analysis
ORDER BY monthly_revenue_at_risk DESC
LIMIT 20;

-- ─── KPI Counter: Customer Summary ───────────────────────────────────────────
SELECT
    SUM(total_customers)                    AS total_customers,
    SUM(churned)                            AS total_churned,
    ROUND(SUM(churned) * 100.0 / SUM(total_customers), 2)   AS overall_churn_rate_pct,
    ROUND(100 - SUM(churned) * 100.0 / SUM(total_customers), 2) AS retention_rate_pct,
    ROUND(AVG(avg_monthly_charges), 2)      AS fleet_arpu,
    ROUND(AVG(avg_clv), 2)                 AS fleet_avg_clv,
    ROUND(SUM(total_customers * avg_monthly_charges), 2) AS total_monthly_revenue
FROM telecom_lakehouse.gold_churn_analysis;
