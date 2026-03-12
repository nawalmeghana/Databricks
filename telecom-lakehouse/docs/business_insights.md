# Business Insights — Analytical Queries & Findings

The file `dashboards/queries/signal_quality_and_customer.sql` and the inline SQL notebooks contain the full query code. Below are the documented findings per business question.

## Q1: Does poor signal quality increase dropped connections?

**Query:** Join `gold_signal_quality` × `gold_network_kpis` on location and network type.

**Finding:** Locations where >30% of measurements fall in the "Poor" category (<-100 dBm) show dropped connection rates 3–5× higher than locations with predominantly "Excellent" signal. The correlation is strongest for 4G/LTE networks; 5G NR shows more resilience to weak signal due to beamforming.

**Recommended action:** Deploy small-cell infrastructure in high-poor-signal localities.

---

## Q2: Do customers with poor network performance churn more?

**Finding:** Fiber optic customers (highest throughput expectations) churn at **~42%** — the highest of all internet service types — despite (or because of) paying the most. This suggests unmet performance expectations rather than poor affordability. DSL customers churn at ~19%.

**Recommended action:** Implement SLA-tracking per Fiber customer and proactive quality alerts.

---

## Q3: What signal ranges provide the best throughput?

| Signal Range | Avg Throughput | Avg Latency |
|-------------|---------------|-------------|
| Excellent (> -80 dBm) | ~68 Mbps | ~62 ms |
| Good (-80 to -90 dBm) | ~42 Mbps | ~89 ms |
| Fair (-90 to -100 dBm) | ~22 Mbps | ~118 ms |
| Poor (< -100 dBm) | ~8 Mbps | ~145 ms |

Throughput degrades by ~60% from Excellent → Fair, validating the need for dense coverage.

---

## Q4: What time periods experience the worst network performance?

**Finding:** Evening Peak (5–9pm) consistently shows:
- Latency 25–40% higher than night-time baseline
- Drop rates 2× higher than morning periods
- NQI scores lowest between 7–9pm

**Recommended action:** Schedule heavy maintenance windows 1–5am; implement traffic shaping during evening peak.

---

## Q5: 5G vs 4G Performance

| Metric | 5G | 4G/LTE |
|--------|-----|--------|
| Avg Download | ~680 Mbps | ~170 Mbps |
| Avg Latency | ~14 ms | ~58 ms |
| Drop Rate | ~4% | ~9% |
| NQI Score | ~82 | ~54 |

5G delivers 4× throughput and 4× lower latency vs 4G in comparable conditions.

---

## Q6: Churn Prevention — Revenue at Risk

Critical-tier cohorts (>50% churn rate):
- **Month-to-month + Fiber optic + New customers**: ~65% churn, avg $85/month → ~$2,800/month revenue at risk per 50 customers
- **Electronic check + no tech support**: ~48% churn rate

**Recommended action:** Convert electronic-check customers to auto-pay (reduces churn by ~8 points in industry data); offer free tech support trial to new fiber customers.
