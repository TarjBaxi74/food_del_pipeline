# Food Delivery Operations Analytics — Solution Document

## 1. Executive Summary

### Problem Statement

A simulated food delivery company operating across multiple cities lacks reliable visibility into:

- **Order Flow**: Volume, completion rates, cancellation patterns
- **Delivery Delays**: SLA breaches, bottleneck identification by city and time slot
- **Refund Leakage**: Root causes and financial impact of refunds
- **Rider Performance**: Efficiency and reliability metrics per rider
- **Operational Trends**: Week-over-week business health indicators

The business team cannot trust existing metrics due to inconsistent definitions, improper joins between systems, and no standardized data models across teams.

### Solution Overview

This project delivers a **local batch analytics pipeline** that:

1. **Generates** realistic synthetic operational data across 7 source domains
2. **Ingests** raw files into a Bronze layer (DuckDB → Parquet) with metadata enrichment
3. **Validates** data quality with explicit profiling and defect flagging before transformation
4. **Transforms** Bronze data into clean, enriched Silver datasets using PySpark
5. **Models** Silver data via dbt into staging, intermediate, and analytical mart layers
6. **Publishes** 5 decision-ready analytics marts for daily business use

### Key Outcomes

| Outcome | Deliverable |
|---------|-------------|
| **Trusted Metrics** | 5 analytics marts with fully documented metric definitions |
| **Data Quality** | Profiling layer with explicit DQ flags at every stage |
| **Reproducibility** | Make-based orchestration, deterministic synthetic data |
| **Self-Service Analytics** | DuckDB warehouse queryable via SQL or notebooks |
| **Engineering Rigor** | Medallion architecture with clear layer responsibilities |

### Current Pipeline Status

| Layer | Technology | Status |
|-------|------------|--------|
| Raw (Data Generation) | Python + Faker | ✅ Complete |
| Bronze (Ingestion) | DuckDB | ✅ Complete |
| Profiling & Validation | Python (custom scripts) | ✅ Complete |
| Silver (Transformation) | PySpark | ✅ Complete |
| Analytics (dbt models) | dbt + DuckDB | ✅ Complete |

---

## 2. Business Context

### Stakeholder Needs

| Stakeholder | Primary Questions | Data Needs |
|-------------|-------------------|------------|
| **Operations Lead** | Which cities and time slots have the worst SLA performance? | SLA breach rates by city, time slot |
| **Restaurant Partnerships** | Which restaurants cause prep-time delays? | Prep time analysis, delay attribution by restaurant |
| **Finance** | How much revenue is lost to refunds? What drives them? | Refund amounts by reason category, weekly trends |
| **Rider Operations** | Who are the top and bottom performing riders? | Efficiency scores, on-time delivery rates |
| **Strategy** | How is the business trending week over week? | Order volumes, GMV, cancellation rate, refund rate |

### Current Pain Points

1. **Inconsistent Definitions**: "On-time delivery" is calculated differently across teams and reports
2. **Improper Joins**: Orders get orphaned from delivery events; refunds lack full order context
3. **No Standardization**: Each function maintains separate spreadsheets with divergent logic
4. **Late-Arriving Data**: Delivery tracking events arrive hours after actual occurrence, causing report discrepancies
5. **Duplicate Records**: The order management system occasionally emits duplicate order records
6. **Orphan Keys**: ~1% of delivery events and refunds reference order IDs that do not exist in the orders table

### Success Criteria

- [x] 7 source datasets generated covering 60+ days of simulated operations
- [x] Bronze layer ingested with metadata enrichment and Parquet storage
- [x] Profiling layer validates quality before Silver transformation
- [x] Silver layer produces clean, enriched, analytics-ready datasets
- [x] All 5 business questions answerable from dbt mart layer
- [x] Metric definitions documented and enforced through dbt tests
- [x] Data quality issues explicitly flagged (not silently dropped)
- [x] Pipeline fully reproducible end-to-end

---

## 3. Data Domain Model

### Entity Relationships

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   RESTAURANTS   │       │     ORDERS      │       │     RIDERS      │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ restaurant_id   │◄──────┤ restaurant_id   │       │ rider_id        │
│ city            │       │ order_id (PK)   │       │ city            │
│ cuisine_type    │       │ customer_id     │       │ shift_type      │
│ rating_band     │       │ city            │       │ joining_date    │
│ onboarding_date │       │ order_ts        │       └────────┬────────┘
└─────────────────┘       │ promised_ts     │                │
                          │ status          │                │
                          │ order_value     │                │
                          │ payment_mode    │                │
                          └───────┬─────────┘                │
                                  │                          │
              ┌───────────────────┼──────────────────────────┤
              │                   │                          │
              ▼                   ▼                          ▼
┌─────────────────┐    ┌─────────────────┐       ┌─────────────────┐
│   ORDER_ITEMS   │    │ DELIVERY_EVENTS │       │    REFUNDS      │
├─────────────────┤    ├─────────────────┤       ├─────────────────┤
│ order_id (FK)   │    │ order_id (FK)   │       │ refund_id (PK)  │
│ item_id         │    │ rider_id (FK)   │       │ order_id (FK)   │
│ quantity        │    │ event_type      │       │ refund_ts       │
│ item_price      │    │ event_ts        │       │ refund_reason   │
│ cuisine_type    │    │ latitude        │       │ refund_amount   │
└─────────────────┘    │ longitude       │       └─────────────────┘
                       └─────────────────┘
                                                  ┌─────────────────┐
                                                  │ SUPPORT_TICKETS │
                                                  ├─────────────────┤
                                                  │ ticket_id (PK)  │
                                                  │ order_id (FK)   │
                                                  │ ticket_type     │
                                                  │ created_ts      │
                                                  │ resolution_status│
                                                  └─────────────────┘
```

### Key Business Concepts

| Concept | Definition |
|---------|------------|
| **Order** | A customer request to purchase and deliver food from a restaurant |
| **SLA** | Service Level Agreement — the promised delivery time given to the customer at order placement |
| **Prep Time** | Duration from `restaurant_accepted` event to `food_ready` event |
| **Delivery Time** | Duration from `rider_picked_up` event to `delivered` event |
| **Time Slot** | Bucketed demand periods: morning (8–11), lunch (12–15), evening (16–19), dinner (20–23) |
| **GMV** | Gross Merchandise Value — total order value for delivered orders |
| **SLA Breach** | Any delivered order where `delivered_ts > promised_delivery_ts` |

### Data Lineage

```
[Raw Files]                 [Bronze Layer]           [Silver Layer]         [dbt / Marts]
     │                           │                        │                       │
orders.csv ─────────────► bronze/orders ──────────► silver/orders ─────┐
order_items.csv ─────────► bronze/order_items ────► silver/order_items  │
delivery_events.json ────► bronze/delivery_events ► silver/delivery ───┼──► mart_sla_breach_analysis
restaurants.csv ─────────► bronze/restaurants ────► silver/restaurants  │    mart_restaurant_prep_delays
riders.csv ──────────────► bronze/riders ─────────► silver/riders ─────┼──► mart_rider_performance
refunds.csv ─────────────► bronze/refunds ────────► silver/refunds ────┼──► mart_refund_drivers
support_tickets.csv ─────► bronze/support_tickets ► silver/tickets ────┘    mart_weekly_trends
```

---

## 4. Metric Definitions

### 4.1 SLA Breach Rate

**Business Question**: Which cities and time slots have the highest delivery SLA breach rate?

**Definition**:
```
SLA Breach Rate (%) = (Orders Delivered After Promised Time / Total Delivered Orders) × 100
```

**Calculation Logic**:
```sql
is_sla_breached = CASE
    WHEN delivered_ts IS NULL THEN NULL        -- Exclude non-delivered orders
    WHEN delivered_ts > promised_delivery_ts THEN TRUE
    ELSE FALSE
END

breach_rate_pct = SUM(is_sla_breached::int) / COUNT(*) * 100
```

**Time Slot Bucketing**:
```sql
time_slot = CASE
    WHEN EXTRACT(hour FROM order_ts) BETWEEN 8  AND 11 THEN 'morning'
    WHEN EXTRACT(hour FROM order_ts) BETWEEN 12 AND 15 THEN 'lunch'
    WHEN EXTRACT(hour FROM order_ts) BETWEEN 16 AND 19 THEN 'evening'
    WHEN EXTRACT(hour FROM order_ts) BETWEEN 20 AND 23 THEN 'dinner'
    ELSE 'late_night'
END
```

**Edge Cases**:
- Orders without `delivered_ts` (cancelled, in-progress) are excluded from the denominator
- Orders with NULL `promised_delivery_ts` are flagged as a data quality issue and excluded
- Breach is measured to the minute with no grace period applied

**Assumptions**:
- Clock synchronization between order management and delivery tracking systems is assumed
- No distinction is made between a 1-minute and a 30-minute breach in the breach rate metric

---

### 4.2 Prep Time Delay

**Business Question**: Which restaurants are causing prep-time delays?

**Definition**:
```
Prep Time (minutes) = food_ready_ts - restaurant_accepted_ts
Prep Delay         = Prep Time > 20 minutes
```

**Calculation Logic**:
```sql
prep_time_minutes = (epoch(food_ready_ts) - epoch(restaurant_accepted_ts)) / 60

delay_rate_pct = SUM(CASE WHEN prep_time_minutes > 20 THEN 1 ELSE 0 END)
                 / COUNT(*) * 100
```

**Risk Categories**:

| Category | Criteria |
|----------|----------|
| High Risk | avg_prep_time > 25 min AND delay_rate > 30% |
| Medium Risk | avg_prep_time > 20 min AND delay_rate > 20% |
| Low Risk | All others |

**Edge Cases**:
- Missing `food_ready` event → order excluded from prep time analysis
- `restaurant_accepted_ts` occurring after `food_ready_ts` → flagged as invalid event sequence, excluded

**Assumptions**:
- The 20-minute threshold is a configurable business rule
- New restaurants (less than 30 days on platform) may have higher prep time variance; noted but not excluded

---

### 4.3 Refund Driver Categories

**Business Question**: What percentage of refunds are driven by delay, missing items, or cancellations?

**Definition**:
```
Refund drivers are categorized into buckets based on the refund_reason field.
```

**Category Mapping**:

| Raw Reason | Category |
|------------|----------|
| `Delay` | Delay |
| `Missing_Items` | Missing Items |
| `Wrong_Order` | Missing Items |
| `Cancellation` | Cancellation |
| `Quality_Issue` | Other |
| NULL | Other |

**Calculation Logic**:
```sql
refund_driver_category = CASE
    WHEN refund_reason = 'Delay'                        THEN 'Delay'
    WHEN refund_reason IN ('Missing_Items','Wrong_Order') THEN 'Missing Items'
    WHEN refund_reason = 'Cancellation'                 THEN 'Cancellation'
    ELSE 'Other'
END

refund_pct = COUNT(category) / COUNT(*) * 100
```

**Validation**:
- Sum of all category percentages must equal 100%
- A custom dbt test (`assert_refund_pct_sums_to_100.sql`) enforces this constraint with a 0.1% tolerance

---

### 4.4 Rider Performance Score

**Business Question**: Which riders consistently handle more orders without increasing late deliveries?

**Definition**:
```
On-Time Rate      = 1 − (Late Deliveries / Total Deliveries)
Efficiency Score  = (Orders Per Day / Avg Orders Per Day) × On-Time Rate
```

**Calculation Logic**:
```sql
on_time_rate = 1.0 - (late_deliveries / NULLIF(total_deliveries, 0))

-- Efficiency combines volume and quality relative to peer average
efficiency_score = (orders_per_day / avg_orders_per_day_in_city) * on_time_rate
```

**Ranking**:
- Riders ranked by `efficiency_score` within their primary city
- Minimum 10 deliveries required in the period for inclusion in rankings

**Edge Cases**:
- Riders with 0 deliveries in the period are excluded entirely
- Riders operating across multiple cities are ranked in their primary city only

---

### 4.5 Weekly Trends

**Business Question**: How do completed orders, cancellations, and refund amounts trend week over week?

**Definition**:
```
Week         = ISO week (Monday 00:00:00 → Sunday 23:59:59)
WoW Change % = (This Week − Last Week) / Last Week × 100
```

**Metrics Tracked**:

| Metric | Calculation |
|--------|-------------|
| Completed Orders | COUNT WHERE status = 'delivered' |
| Cancellations | COUNT WHERE status = 'cancelled' |
| Cancellation Rate | Cancelled / Total Orders × 100 |
| Total GMV | SUM(order_value) WHERE status = 'delivered' |
| Average Order Value | AVG(order_value) WHERE status = 'delivered' |
| Refund Count | COUNT(refund_id) |
| Refund Amount | SUM(refund_amount) |
| Refund Rate | Refund Amount / GMV × 100 |

**WoW Metrics Applied To**: completed orders, cancelled orders, GMV, refund amount.

**Week Boundaries**:
- ISO week standard: Monday start
- Partial weeks at the boundaries of the data range are included but can be flagged

---

## 5. Design Decisions & Trade-offs

### 5.1 Why DuckDB for Bronze (Not PySpark)?

| Factor | DuckDB | PySpark |
|--------|--------|---------|
| **Setup** | Zero config, embedded, no JVM | Requires JVM, Spark session setup |
| **CSV/JSON Ingestion** | Native, fast | Requires schema definition, schema inference |
| **Parquet Output** | Native | Native but higher overhead |
| **Local Perf (Small Data)** | Columnar, extremely fast | Overhead not justified at this scale |
| **dbt Compatibility** | Native dbt-duckdb adapter | Separate tool chain |

**Decision**: DuckDB provides equivalent schema enforcement, Parquet output, and metadata enrichment for the ingestion use case at zero infrastructure cost. PySpark is reserved for the Silver layer where distributed transformation logic (pivoting, deduplication, enrichment) genuinely benefits from Spark's API.

**Trade-off**: The Bronze layer does not use the same engine as Silver. This is a deliberate pragmatic choice — not every layer needs the same technology.

---

### 5.2 Why PySpark for Silver?

The Silver layer involves:
- Lifecycle event **pivoting** (8 event types → single row per order)
- Multi-dataset **joins** (orders × delivery timeline × refunds)
- Complex **derived columns** (prep time, SLA breach flag, delivery durations)
- **Deduplication** with deterministic window functions

PySpark's DataFrame API and window function support make these transformations readable, testable, and scalable. The Silver layer is the most complex transformation stage and benefits from Spark's expressiveness.

---

### 5.3 Why Medallion Architecture (Bronze / Silver / Marts)?

```
Bronze : Raw data preservation — audit trail, reprocessing capability, metadata lineage
Silver : Clean data — consistent types, deduplication, derived columns, DQ flags
Marts  : Business logic — aggregations, metric definitions, consumption-ready
```

**Trade-off**: Additional storage and compute per layer vs. debugging capability and explicit data quality visibility.

**Decision**: Explicit layers make data quality issues visible at the point of origin rather than silently fixing them downstream. Each layer can be independently reprocessed.

---

### 5.4 Why dbt for the Modelling Layer?

- **Documentation**: Built-in docs generation with column-level descriptions
- **Testing**: Native schema tests (unique, not_null, relationships) and custom SQL tests
- **Lineage**: Automatic DAG visualization showing model dependencies
- **Modularity**: SQL-based, version-controlled, easy to review and extend
- **Ecosystem**: dbt_utils, data contracts, seeds for reference data

**Alternative Considered**: Pure PySpark for all layers → Rejected because dbt provides purpose-built testing, documentation, and modelling conventions that PySpark lacks natively.

---

### 5.5 Handling Late-Arriving Delivery Events

**Problem**: A subset of delivery tracking events arrive hours after their actual occurrence time, causing report discrepancies when events are processed by ingestion timestamp.

**Approach**:
1. **Bronze**: All events ingested with `_ingested_at` metadata timestamp preserved
2. **Silver**: Events ordered by `event_ts` (actual event time), not `_ingested_at`
3. **Reprocessing**: Daily batch designed to reprocess recent history to catch late arrivals

**Trade-off**: Storing ingestion metadata increases storage slightly but enables forensic debugging of late-arrival patterns.

---

### 5.6 Handling Orphan Records and Data Quality Defects

**Problem**: ~1% of delivery events and refunds reference `order_id` values that have no match in the orders table.

**Approach**: Flag, don't drop.

```python
# Silver layer — orphan detection
df = df.withColumn(
    "_dq_orphan_order",
    ~col("order_id").isin(valid_order_ids)
)
```

Records are flagged with `_dq_*` columns at the Silver layer and **excluded from mart aggregations** via staging model filters. This keeps the audit trail intact while ensuring marts contain only clean data.

**Rationale**: Silently dropping records hides upstream system issues. Explicit flagging enables investigation and trend monitoring of defect rates over time.

---

## 6. Data Quality Approach

### 6.1 Injected Defects (Why?)

Synthetic data is generated with intentional quality issues to validate that the pipeline handles them correctly rather than silently passing bad data through.

| Defect Type | Approx. Rate | Purpose |
|-------------|-------------|---------|
| Null values | ~2% | Validate null handling in Silver transforms |
| Duplicate order records | ~0.5% | Validate deduplication logic |
| Late-arriving events | ~3% | Validate event ordering logic in Silver |
| Orphan foreign keys | ~1% | Validate referential integrity checks |
| Missing payment modes | Occasional | Validate payment mode correction logic |
| SLA breaches | ~12% | Realistic operational scenario for breach analysis |
| Cancellations | Variable | Validate cancellation rate calculations |

### 6.2 Profiling Layer (Pre-Silver)

Before Silver transformations run, dedicated profiling scripts validate Bronze outputs:

| Check | Scope |
|-------|-------|
| Null percentage per column | All Bronze datasets |
| Duplicate order detection | orders Bronze table |
| Delivery event lifecycle completeness | delivery_events Bronze table |
| Orphan refund identification | refunds Bronze table |
| Delay and SLA distribution analysis | orders + delivery_events |

Purpose: Surface data issues explicitly before business transformation logic runs. Profiling results are logged and can feed a monitoring dashboard.

### 6.3 DQ Flags at Silver Layer

Every Silver table includes `_dq_*` boolean flag columns:

| Column | Table | Meaning |
|--------|-------|---------|
| `_dq_has_null_customer` | silver.orders | customer_id is null |
| `_dq_invalid_order_value` | silver.orders | order_value is null or ≤ 0 |
| `_dq_orphan_order` | silver.delivery_events | order_id not found in orders |
| `_dq_orphan_order` | silver.refunds | order_id not found in orders |
| `_dq_invalid_sequence` | silver.delivery_events | event timestamps out of expected order |
| `_dq_missing_payment_mode` | silver.orders | payment_mode is null after correction attempt |

### 6.4 dbt Layer Tests

| Layer | Test Type | Examples |
|-------|-----------|---------|
| Staging | Schema tests | unique + not_null on all PKs, not_null on critical fields |
| Staging | Relationship tests | stg_orders.restaurant_id → stg_restaurants.restaurant_id |
| Marts | Custom SQL test | `assert_refund_pct_sums_to_100.sql` — percentages sum to 100% ±0.1% |

---

## 7. Assumptions & Limitations

### Assumptions

1. **Batch Processing Only**: Data is refreshed daily; no real-time requirements exist for this project
2. **Single Timezone**: All timestamps are assumed to be in IST (India Standard Time)
3. **Static Master Data**: Restaurant and rider master records do not change retroactively
4. **Synthetic Data**: Generated data approximates realistic patterns but does not replicate a production system
5. **Single-City Riders**: Riders are primarily assigned to one city for ranking purposes
6. **No Grace Period**: SLA breach is binary — any delivery after promised time is a breach

### Limitations

1. **No Streaming**: Late-arriving events require batch reprocessing of recent history
2. **Local Execution Only**: Not designed for distributed or cloud execution at this stage
3. **Full Refresh**: All layers run full refreshes on each pipeline execution; no incremental processing
4. **dbt Layer In Progress**: Analytics marts are planned and partially scaffolded, not yet fully materialised
5. **Limited History**: 60 days of simulation may not capture seasonal or long-term cyclical patterns

### Future Enhancements

- [ ] Incremental processing with watermark-based ingestion for large-scale datasets
- [ ] Real-time streaming pipeline using Kafka + Spark Structured Streaming
- [ ] ML-based anomaly detection for data quality monitoring
- [ ] Dashboard layer integration (Metabase or Apache Superset)
- [ ] Complete dbt mart layer with full test coverage and documentation
- [ ] CI/CD pipeline for automated testing on every commit