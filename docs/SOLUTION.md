# Food Delivery Operations Analytics — Solution Document

## 1. Executive Summary

### Problem Statement

A food delivery company operating across multiple Indian cities lacks reliable visibility into:

- **Order Flow** — volume, completion rates, cancellation patterns
- **Delivery Delays** — SLA breaches and bottleneck identification
- **Refund Leakage** — root causes driving refunds
- **Rider Performance** — efficiency and reliability metrics

Business teams do not trust existing metrics due to inconsistent definitions, improper joins between systems, and the absence of standardised data models.

### Solution Overview

This project delivers a **local batch analytics pipeline** that:

1. **Ingests** raw operational data from 7 source systems
2. **Validates** data quality with explicit defect handling at every layer
3. **Transforms** raw data into clean, analytics-ready warehouse tables
4. **Publishes** five decision-ready marts for daily business use

### Key Outcomes

| Outcome | Deliverable |
|---------|-------------|
| Trusted Metrics | 5 analytics marts with documented definitions |
| Data Quality | DQ flags at every layer; explicit handling of nulls, duplicates, orphans |
| Reproducibility | Single-command orchestration; deterministic data generation |
| Self-Service Analytics | DuckDB warehouse queryable directly via SQL |

---

## 2. Business Context

### Stakeholder Needs

| Stakeholder | Primary Questions | Data Needs |
|-------------|-------------------|------------|
| Operations Lead | Which cities/time slots have SLA issues? | SLA breach rates by city and time slot |
| Restaurant Partnerships | Which restaurants cause delays? | Prep time analysis, delay attribution |
| Finance | How much are we losing to refunds? | Refund amounts by reason, weekly trends |
| Rider Operations | Who are top/bottom performers? | Rider efficiency scores, on-time rates |
| Strategy | How is the business trending? | Weekly order / revenue / cancellation trends |

### Current Pain Points

1. **Inconsistent Definitions** — "On-time delivery" is calculated differently across teams
2. **Improper Joins** — Orders orphaned from delivery events; refunds missing order context
3. **No Standardisation** — Each team maintains separate spreadsheets with diverging numbers
4. **Late-Arriving Data** — Delivery tracking events arrive hours late, causing report discrepancies
5. **Duplicate Records** — Order management system occasionally emits duplicate rows

### Success Criteria

- All 5 business questions answerable from marts
- Metric definitions documented and agreed upon
- Data quality issues explicitly flagged, not silently dropped
- Pipeline reproducible with `python -m src.pipeline_runner`
- 60+ days of historical data available for trend analysis

---

## 3. Data Domain Model

### Entity Relationships

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   RESTAURANTS   │       │     ORDERS      │       │     RIDERS      │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ restaurant_id   │◄──────┤ restaurant_id   │       │ rider_id        │
│ city            │       │ order_id  (PK)  │       │ city            │
│ cuisine_type    │       │ customer_id     │       │ shift_type      │
│ rating_band     │       │ city            │       │ joining_date    │
│ onboarding_date │       │ order_ts        │       └────────┬────────┘
└─────────────────┘       │ promised_ts     │                │
                          │ status          │                │
                          │ order_value     │                │
                          │ payment_mode    │                │
                          └───────┬─────────┘                │
                                  │                          │
              ┌───────────────────┼──────────────────────────┘
              │                   │
              ▼                   ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   ORDER_ITEMS   │    │ DELIVERY_EVENTS  │    │    REFUNDS      │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ order_id  (FK)  │    │ order_id   (FK)  │    │ refund_id  (PK) │
│ item_id         │    │ rider_id   (FK)  │    │ order_id   (FK) │
│ quantity        │    │ event_type       │    │ refund_ts       │
│ item_price      │    │ event_ts         │    │ refund_reason   │
│ cuisine_type    │    │ latitude         │    │ refund_amount   │
└─────────────────┘    │ longitude        │    └─────────────────┘
                       └──────────────────┘
                                                ┌─────────────────┐
                                                │ SUPPORT_TICKETS │
                                                ├─────────────────┤
                                                │ ticket_id  (PK) │
                                                │ order_id   (FK) │
                                                │ ticket_type     │
                                                │ created_ts      │
                                                │ resolution_status│
                                                └─────────────────┘
```

### Key Business Concepts

| Concept | Definition |
|---------|------------|
| Order | A customer request to purchase and deliver food from a restaurant |
| SLA | Promised delivery time given to the customer at order placement |
| Prep Time | Duration from restaurant accepting the order to food being ready |
| Delivery Time | Duration from rider pickup to customer delivery |
| Time Slot | morning (8–11h), lunch (12–15h), evening (16–19h), dinner (20–23h) |

### Data Lineage

```
[Raw Files]              [Bronze Layer]          [Silver Layer]       [Marts]
    │                         │                       │                  │
orders.csv ─────────► bronze/orders ────────► silver/orders ──────┐
order_items.csv ─────► bronze/order_items ──► silver/order_items  │
delivery_events.json ► bronze/delivery ─────► silver/delivery ────┼──► mart_sla_breach_analysis
restaurants.csv ─────► bronze/restaurants ──► silver/restaurants  │    mart_restaurant_prep_delays
riders.csv ──────────► bronze/riders ───────► silver/riders ──────┼──► mart_rider_performance
refunds.csv ─────────► bronze/refunds ──────► silver/refunds ─────┼──► mart_refund_drivers
support_tickets.csv ─► bronze/tickets ──────► silver/tickets ─────┘    mart_weekly_trends
```

---

## 4. Metric Definitions

### 4.1 SLA Breach Rate

**Business Question:** Which cities and time slots have the highest delivery SLA breach rate?

**Definition:**
```
SLA Breach Rate (%) = (Orders delivered after promised time / Total delivered orders) × 100
```

**Calculation:**
```sql
is_sla_breached = CASE
    WHEN delivered_ts IS NULL         THEN NULL   -- exclude non-delivered
    WHEN delivered_ts > promised_ts   THEN TRUE
    ELSE FALSE
END

breach_rate_pct = SUM(is_sla_breached) * 1.0 / COUNT(*) * 100
```

**Edge Cases:**
- Cancelled and in-progress orders are excluded from the denominator
- Orders with NULL `promised_delivery_ts` are flagged as a DQ issue and excluded
- No grace period — breach measured to the minute

**Assumptions:**
- Clock synchronisation between order and delivery tracking systems is assumed
- No distinction is made between a 1-minute and a 30-minute breach in the rate metric

---

### 4.2 Restaurant Prep Time Delay

**Business Question:** Which restaurants cause prep-time delays that push deliveries beyond target?

**Definition:**
```
Prep Time (minutes)  = food_ready_ts − restaurant_accepted_ts
Prep Delay threshold = 20 minutes
```

**Calculation:**
```sql
prep_time_minutes = (epoch(food_ready_ts) - epoch(restaurant_accepted_ts)) / 60

delay_rate_pct = SUM(CASE WHEN prep_time_minutes > 20 THEN 1 ELSE 0 END)
                 * 100.0 / COUNT(*)
```

**Risk Categories:**

| Category | Criteria |
|----------|----------|
| High Risk | avg_prep_time > 25 min AND delay_rate > 30% |
| Medium Risk | avg_prep_time > 20 min AND delay_rate > 20% |
| Low Risk | All others |

**Minimum volume:** 10 orders required for a restaurant to appear in the mart.

**Edge Cases:**
- Missing `food_ready` event → excluded from prep time calculation
- `restaurant_accepted_ts` later than `food_ready_ts` → flagged `_dq_invalid_sequence`

---

### 4.3 Refund Driver Categories

**Business Question:** What percentage of refunds are driven by delay, missing items, or cancellations?

**Category Mapping:**

| Raw Reason | Mapped Category |
|------------|----------------|
| Delay | Delay |
| Missing_Items | Missing Items |
| Wrong_Order | Missing Items |
| Cancellation | Cancellation |
| Quality_Issue | Other |
| NULL | Other |

**Calculation:**
```sql
refund_count_pct   = COUNT(category) * 100.0 / SUM(COUNT(*)) OVER ()
refund_amount_pct  = SUM(refund_amount) * 100.0 / SUM(SUM(refund_amount)) OVER ()
```

**Validation:** Sum of all category percentages must equal 100% (±0.1% tolerance). Enforced by `assert_refund_pct_sums_to_100.sql`.

---

### 4.4 Rider Performance Score

**Business Question:** Which riders consistently handle more orders without increasing late deliveries?

**Definition:**
```
On-Time Rate       = 1 − (Late Deliveries / Total Deliveries)
Efficiency Score   = (Orders Per Day / City Avg Orders Per Day) × On-Time Rate
```

**Performance Tiers:**

| Tier | Criteria |
|------|----------|
| Star Performer | on_time_rate ≥ 0.95 AND orders_per_day ≥ city average |
| Meets Expectations | on_time_rate ≥ 0.90 |
| Needs Improvement | on_time_rate ≥ 0.80 |
| At Risk | on_time_rate < 0.80 |

**Minimum volume:** 10 deliveries required for ranking inclusion.

**Edge Cases:**
- Riders with zero deliveries in the period are excluded
- Riders covering multiple cities are ranked within their primary city

---

### 4.5 Weekly Trends

**Business Question:** How do completed orders, cancellations, and refund amounts trend week over week?

| Metric | Calculation |
|--------|-------------|
| Completed Orders | COUNT WHERE status = 'delivered' |
| Cancellations | COUNT WHERE status = 'cancelled' |
| Total GMV | SUM(order_value) WHERE status = 'delivered' |
| Refund Amount | SUM(refund_amount) |
| Cancellation Rate | Cancellations / Total Orders × 100 |
| Refund Rate | Refund Amount / GMV × 100 |
| WoW Change | (This Week − Last Week) / Last Week × 100 |

**Week boundary:** ISO week — Monday 00:00:00 to Sunday 23:59:59.

---

## 5. Design Decisions & Trade-offs

### 5.1 Medallion Architecture

```
Bronze — Raw data preserved exactly as received; audit trail; reprocessing capability
Silver — Deduplicated, typed, null-handled, enriched; DQ flags visible, not hidden
Marts  — Business logic and aggregations; consumption-ready
```

**Trade-off:** Additional storage and compute versus debugging capability and data quality transparency.

**Decision:** Explicit layers make issues visible. Silently fixing data in a single transformation step hides upstream problems from the business.

---

### 5.2 DuckDB for Warehouse

DuckDB was chosen over Postgres or a managed warehouse for the following reasons:

- Zero infrastructure — embedded, file-based, no server required
- Columnar engine optimised for OLAP queries
- Native Parquet read/write support
- First-class `dbt-duckdb` adapter
- Fully sufficient at the data volumes of this project

**Trade-off:** Not suitable for concurrent write workloads or production multi-user access; appropriate for local and dev-tier analytics pipelines.

---

### 5.3 PySpark for Silver Transformations

PySpark provides schema enforcement, partition-based output, and a familiar distributed API even in local mode. For the bronze layer, DuckDB is used instead — it has equivalent capabilities at this volume without JVM startup overhead.

---

### 5.4 Late-Arriving Events

**Problem:** ~3% of delivery events arrive 1–24 hours after actual occurrence.

**Approach:**
- Bronze: All events ingested with `_ingested_at` timestamp preserved
- Silver: Events ordered by `event_ts` (actual time), not ingestion time
- Full refresh on each pipeline run covers late arrivals from the prior day

**Trade-off:** Storage cost of full refresh versus accuracy of event ordering.

---

### 5.5 Orphan Records

**Problem:** ~1% of delivery events reference non-existent `order_id` values.

**Approach:** Flag as `_dq_orphan_order = TRUE` rather than silently dropping. Filtered out in staging layer before reaching marts. Rate monitored via DQ queries.

**Rationale:** Dropping records silently hides upstream system issues; flagging enables investigation and escalation.

---

## 6. Data Quality Approach

### 6.1 Injected Defects (Rationale)

| Defect Type | Rate | Purpose |
|-------------|------|---------|
| Nulls | 2% | Validates null handling in transformations |
| Duplicates | 0.5% | Validates deduplication logic |
| Late Events | 3% | Validates event ordering logic |
| Orphan Keys | 1% | Validates referential integrity checks |
| SLA Breaches | 12% | Realistic operational failure scenario |

---

### 6.2 DQ Checks by Layer

| Layer | Checks Performed |
|-------|-----------------|
| Bronze | Schema validation, corrupt record capture via PERMISSIVE mode |
| Silver | Deduplication, null handling, orphan detection, valid value ranges |
| dbt Staging | Unique PKs, not-null on critical fields |
| dbt Marts | Business rule validation (e.g. percentages sum to 100) |

---

### 6.3 Monitoring

DQ flags available on every silver table:

```sql
SELECT
    COUNT(*)                             AS total_records,
    SUM(_dq_has_null_customer::int)      AS null_customer_count,
    SUM(_dq_invalid_order_value::int)    AS invalid_value_count,
    SUM(_dq_orphan_order::int)           AS orphan_count,
    SUM(_dq_invalid_sequence::int)       AS invalid_sequence_count
FROM silver.orders;
```

---

## 7. Assumptions & Limitations

### Assumptions

- All timestamps are in IST (no timezone conversion required)
- Batch processing only — no real-time requirements
- Master data (restaurants, riders) is static within a pipeline run
- Synthetic data approximates real operational patterns

### Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| No incremental processing | Full refresh on each run | Acceptable at this data volume |
| Local execution only | Not distributed | DuckDB handles analytical load well locally |
| No streaming | Late events require next-day reprocessing | Full refresh covers this |
| 60-day window | May miss seasonal patterns | Sufficient for capstone demonstration |

### Future Enhancements

- Incremental dbt models for large-scale datasets
- Streaming ingestion with Kafka + Spark Structured Streaming
- ML-based anomaly detection for DQ monitoring
- Dashboard layer via Metabase or Apache Superset
- Airflow/Prefect for production scheduling