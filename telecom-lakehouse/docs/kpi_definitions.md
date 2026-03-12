# Telecom KPI Definitions

## Network KPIs

### Network Quality Index (NQI)
**Formula:** `(100 - avg_latency_ms) × 0.3 + (avg_download_mbps / 10) × 0.4 + (10 - avg_jitter_ms) × 0.3`
**Range:** 0 – 100 (higher = better)
**Interpretation:** Composite score weighting latency (30%), throughput (40%), and jitter (30%).
Mirrors the ITU-T G.1028 quality model for broadband network assessment.

### Dropped Connection Rate (DCR)
**Formula:** `(dropped_connections / total_sessions) × 100`
**Unit:** Percentage (%)
**Industry benchmark:** < 2% is considered excellent; > 5% is problematic
**Source column:** `dropped_connection` (boolean per session)

### Latency Percentiles (P50/P95/P99)
- **P50 (median latency):** 50% of sessions experience this latency or better
- **P95:** 95th percentile — captures tail latency experienced by 5% of users
- **P99:** 99th percentile — worst-case latency (important for real-time services like VoNR)
**Industry benchmark (5G):** P50 < 10ms, P99 < 30ms for eMBB use cases

### Throughput Performance
**Download Mbps tiers:**
- Ultra: ≥ 500 Mbps
- High: ≥ 100 Mbps
- Medium: ≥ 25 Mbps
- Low: < 25 Mbps

### VoNR Penetration Rate
**Formula:** `(vonr_enabled_sessions / total_5G_sessions) × 100`
VoNR (Voice over New Radio) adoption rate — key 5G monetization metric.

---

## Signal KPIs

### Signal Strength (dBm) — RSRP Equivalent
| Range | Category | User Experience |
|-------|----------|----------------|
| > -80 dBm | Excellent | Full bars, peak speeds |
| -80 to -90 dBm | Good | Normal usage, HD streaming |
| -90 to -100 dBm | Fair | Reduced speeds, possible drops |
| < -100 dBm | Poor | Frequent drops, unusable |

### Signal Score (0–100 normalized)
**Formula:** `((signal_strength_dbm + 140) / 100) × 100`
Maps -140 dBm (worst) → 0, -40 dBm (best) → 100.

### Signal Quality Category Distribution
Percentage of measurements per category. Dashboard shows stacked bar by location.
Goal: > 80% measurements in Excellent or Good.

---

## Customer KPIs

### Churn Rate
**Formula:** `(churned_customers / total_customers) × 100`
**Industry benchmark:** 1.5–2.5% monthly for US telecom operators.
**This dataset:** ~26.5% total (period churn — not monthly).

### Customer Lifetime Value (CLV)
**Formula:** `monthly_charges × tenure_months`
Simple CLV based on observed revenue. Advanced CLV would incorporate discount rate and predicted future tenure.

### ARPU (Average Revenue Per User)
**Formula:** `total_monthly_revenue / total_active_customers`
Key metric for telecom financial reporting. Tracked by contract type, service bundle, and network type.

### Retention Rate
**Formula:** `100 - churn_rate_pct`
Complement of churn rate. Higher retention = better customer satisfaction and network quality.

### Churn Risk Score
**Formula (composite):**
```
contract_score  = 40 if Month-to-month else 10
tenure_score    = 30 if ≤6m | 20 if ≤12m | 5 otherwise
revenue_score   = 20 if monthly_charges > $70 else 10
service_score   = 10 if Fiber optic else 0
total           = contract_score + tenure_score + revenue_score + service_score
```
**Range:** 0–100. Used for proactive retention campaign targeting.

---

## Business Insights from KPIs

1. **Fiber optic customers churn at ~42% vs DSL ~19%** — likely due to service quality expectations
2. **Month-to-month contracts drive 43% churn vs 11% for 2-year contracts**
3. **New customers (0–6m tenure) are 3× more likely to churn than established (25–48m)**
4. **Customers with high monthly charges ($70+) and no tech support have highest CLV risk**
5. **5G connections show 60% lower dropped connection rates vs 4G in same locations**
