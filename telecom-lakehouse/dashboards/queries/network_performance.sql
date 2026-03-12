-- ============================================================
-- Dashboard: Network Performance
-- Databricks SQL Queries
-- ============================================================

-- ─── Chart 1: Average Latency Over Time (Hourly) ─────────────────────────────
SELECT
    event_date,
    event_hour,
    CONCAT(event_date, ' ', LPAD(event_hour, 2, '0'), ':00') AS datetime_label,
    location,
    network_type,
    avg_latency_ms,
    p95_latency_ms,
    p99_latency_ms
FROM telecom_lakehouse.gold_network_kpis
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY event_date, event_hour, location;

-- ─── Chart 2: Throughput Trends by Network Type ──────────────────────────────
SELECT
    event_date,
    network_type,
    ROUND(AVG(avg_download_mbps), 2)    AS avg_download_mbps,
    ROUND(AVG(avg_upload_mbps),   2)    AS avg_upload_mbps,
    ROUND(MAX(max_download_mbps), 2)    AS peak_download_mbps,
    ROUND(AVG(total_data_usage_mb), 2)  AS avg_total_data_mb
FROM telecom_lakehouse.gold_network_kpis
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY event_date, network_type
ORDER BY event_date, network_type;

-- ─── Chart 3: Dropped Connection Rate by Location ────────────────────────────
SELECT
    location,
    network_type,
    carrier,
    ROUND(AVG(dropped_connection_rate_pct), 2) AS avg_drop_rate_pct,
    SUM(dropped_connections)                    AS total_drops,
    SUM(total_sessions)                         AS total_sessions,
    ROUND(AVG(avg_jitter_ms), 2)               AS avg_jitter_ms
FROM telecom_lakehouse.gold_network_kpis
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY location, network_type, carrier
ORDER BY avg_drop_rate_pct DESC;

-- ─── Chart 4: Network Health Score Ranking ───────────────────────────────────
SELECT
    location,
    network_type,
    carrier,
    ROUND(AVG(network_health_score),         2) AS avg_health_score,
    ROUND(AVG(avg_network_quality_index),    2) AS avg_nqi,
    ROUND(AVG(avg_download_mbps),            2) AS avg_download,
    ROUND(AVG(avg_latency_ms),               2) AS avg_latency,
    ROUND(AVG(dropped_connection_rate_pct),  2) AS avg_drop_rate
FROM telecom_lakehouse.gold_network_kpis
GROUP BY location, network_type, carrier
ORDER BY avg_health_score DESC
LIMIT 20;

-- ─── Chart 5: Peak Hour Heatmap ──────────────────────────────────────────────
SELECT
    event_hour                              AS hour_of_day,
    ROUND(AVG(avg_latency_ms),          2)  AS avg_latency_ms,
    ROUND(AVG(avg_download_mbps),       2)  AS avg_download_mbps,
    ROUND(AVG(dropped_connection_rate_pct), 2) AS avg_drop_rate,
    SUM(total_sessions)                     AS total_sessions,
    ROUND(AVG(avg_network_quality_index), 2) AS avg_nqi
FROM telecom_lakehouse.gold_network_kpis
GROUP BY event_hour
ORDER BY event_hour;

-- ─── Chart 6: Carrier Comparison ─────────────────────────────────────────────
SELECT
    carrier,
    network_type,
    ROUND(AVG(avg_download_mbps),            2) AS avg_download_mbps,
    ROUND(AVG(avg_latency_ms),               2) AS avg_latency_ms,
    ROUND(AVG(dropped_connection_rate_pct),  2) AS avg_drop_rate_pct,
    ROUND(AVG(vonr_penetration_pct),         2) AS vonr_penetration_pct,
    ROUND(AVG(avg_video_quality_score),      2) AS avg_video_quality,
    SUM(total_sessions)                         AS total_sessions
FROM telecom_lakehouse.gold_network_kpis
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY carrier, network_type
ORDER BY avg_download_mbps DESC;

-- ─── Chart 7: VoNR Adoption Over Time ────────────────────────────────────────
SELECT
    event_date,
    network_type,
    ROUND(AVG(vonr_penetration_pct), 2) AS vonr_penetration_pct,
    SUM(total_sessions)                  AS total_sessions
FROM telecom_lakehouse.gold_network_kpis
WHERE network_type IN ('5G', '5G NSA')
GROUP BY event_date, network_type
ORDER BY event_date;

-- ─── KPI Counter: Overall Network Summary ────────────────────────────────────
SELECT
    ROUND(AVG(avg_download_mbps),            1)  AS fleet_avg_download_mbps,
    ROUND(AVG(avg_latency_ms),               1)  AS fleet_avg_latency_ms,
    ROUND(AVG(dropped_connection_rate_pct),  2)  AS fleet_drop_rate_pct,
    ROUND(AVG(avg_network_quality_index),    2)  AS fleet_avg_nqi,
    SUM(total_sessions)                          AS total_sessions,
    COUNT(DISTINCT location)                     AS locations_monitored,
    COUNT(DISTINCT carrier)                      AS carriers_tracked
FROM telecom_lakehouse.gold_network_kpis
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 30);
