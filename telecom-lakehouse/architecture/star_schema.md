# Star Schema Data Model

## Overview

The Gold layer is organized as a **Star Schema** optimized for analytical queries.
Fact tables hold measurable events; dimension tables hold descriptive attributes.

## Entity Relationship

```
                    ┌─────────────┐
                    │  dim_time   │
                    │─────────────│
                    │ time_key PK │
                    │ date        │
                    │ hour        │
                    │ day_of_week │
                    │ month       │
                    │ quarter     │
                    │ year        │
                    │ is_weekend  │
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
┌──────────────────┐ ┌────────────────┐ ┌──────────────────┐
│fact_signal_metric│ │fact_net_perform│ │fact_customer_usag│
│──────────────────│ │────────────────│ │──────────────────│
│signal_id PK      │ │perf_id PK      │ │usage_id PK       │
│time_key FK       │ │time_key FK     │ │time_key FK       │
│device_key FK     │ │device_key FK   │ │customer_key FK   │
│cell_key FK       │ │cell_key FK     │ │                  │
│rsrp              │ │latency_ms      │ │monthly_charges   │
│rsrq              │ │download_mbps   │ │total_charges     │
│sinr              │ │upload_mbps     │ │tenure_months     │
│signal_strength   │ │jitter_ms       │ │churn_flag        │
│throughput_mbps   │ │dropped_conn    │ │contract_type     │
│latency_ms        │ │packet_loss_pct │ │payment_method    │
│network_type      │ │network_type    │ └──────────────────┘
└──────────────────┘ └────────────────┘
          │                │
          ▼                ▼
┌──────────────────┐ ┌──────────────────┐
│   dim_device     │ │  dim_network_cell│
│──────────────────│ │──────────────────│
│ device_key PK    │ │ cell_key PK      │
│ device_model     │ │ location         │
│ carrier          │ │ latitude         │
│ band             │ │ longitude        │
│ vonr_enabled     │ │ network_type     │
│ os_type          │ │ carrier          │
└──────────────────┘ │ band             │
                     └──────────────────┘
          ▲
          │
┌──────────────────┐
│   dim_customer   │
│──────────────────│
│ customer_key PK  │
│ customer_id      │
│ gender           │
│ senior_citizen   │
│ partner          │
│ dependents       │
│ internet_service │
│ streaming_tv     │
│ streaming_movies │
└──────────────────┘
```

## Relationships

- `fact_signal_metrics` → `dim_time` (many:1)
- `fact_signal_metrics` → `dim_device` (many:1)
- `fact_signal_metrics` → `dim_network_cell` (many:1)
- `fact_network_performance` → `dim_time` (many:1)
- `fact_network_performance` → `dim_device` (many:1)
- `fact_network_performance` → `dim_network_cell` (many:1)
- `fact_customer_usage` → `dim_time` (many:1)
- `fact_customer_usage` → `dim_customer` (many:1)
