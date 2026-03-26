# Food Delivery Operations Analytics Pipeline — Architecture Reference

## Objective

Design and implement an end-to-end data engineering and analytics pipeline using Medallion Architecture to generate operational insights for a simulated food delivery platform.

Key analytical goals:

- Identifying cities and time slots with highest SLA breaches
- Detecting restaurant-driven delivery delays
- Understanding refund drivers
- Evaluating rider performance
- Tracking weekly operational trends

---

## Architecture Overview

The pipeline follows a layered Medallion design:

```
Raw Layer → Bronze Layer → Silver Layer → Analytics Layer (dbt)
```

Each layer progressively improves data quality, structure, and business usability.

| Layer | Technology | Status |
|---|---|---|
| Raw (Data Generation) | Python | ✅ Complete |
| Bronze (Ingestion) | DuckDB | ✅ Complete |
| Profiling & Validation | Python (custom scripts) | ✅ Complete |
| Silver (Transformation) | PySpark | ✅ Complete |
| Analytics (dbt models) | dbt + DuckDB | 🔄 In Progress |

---

## Raw Layer — Data Generation

**Location:** `src/generators/` → `data/raw/`

Custom Python generators simulate operational datasets stored as CSV and JSONL.

Datasets generated:

- `orders.csv` — order_id, customer_id, restaurant_id, city, order_ts, promised_delivery_ts, status, order_value, payment_mode
- `order_items.csv` — order_id, item_id, quantity, item_price, cuisine_type
- `delivery_events.json` — order_id, rider_id, event_type, event_ts, latitude, longitude
- `restaurants.csv` — restaurant_id, city, cuisine_type, rating_band, onboarding_date
- `riders.csv` — rider_id, city, shift_type, joining_date
- `refunds.csv` — refund_id, order_id, refund_ts, refund_reason, refund_amount
- `support_tickets.csv` — ticket_id, order_id, ticket_type, created_ts, resolution_status

Key characteristics:

- Realistic peak-hour demand patterns
- Lifecycle-based delivery event generation
- Injected data quality noise — missing payment modes, late deliveries, cancellations
- Minimum 60 days of simulated operations
- Realistic nulls, duplicates, late events, and mismatched keys

---

## Bronze Layer — Ingestion

**Location:** `src/loaders/` → `data/bronze/`

**Technology:** DuckDB

Raw datasets are ingested into DuckDB and written to Parquet format to create a lightweight local data lake.

Transformations performed:

- Schema normalization across all source files
- Ingestion metadata enrichment — `ingested_at`, `load_date`, `source_file`
- Standardized Parquet storage format per dataset

Output structure:

```
data/bronze/
├── orders/
├── order_items/
├── delivery_events/
├── restaurants/
├── riders/
├── refunds/
└── support_tickets/
```

**Note on technology choice:** DuckDB was selected over PySpark for the bronze ingestion layer due to its high-performance local analytical engine, native CSV and JSON ingestion, and significantly lower setup overhead for local pipeline prototyping. This is a deliberate engineering tradeoff — DuckDB provides equivalent schema enforcement, Parquet output, and metadata enrichment at this data volume without JVM dependency.

---

## Data Profiling and Quality Validation

**Location:** `src/profiling/`

Before business transformations, Bronze datasets are validated using custom profiling scripts.

Quality checks implemented:

- Null percentage per column across all datasets
- Duplicate order detection
- Delivery event lifecycle completeness validation
- Orphan refund identification (refunds with no matching order)
- Delay distribution analysis

Purpose: Ensure curated and reliable inputs for downstream PySpark transformation layer.

---

## Silver Layer — Business Transformations

**Location:** `src/spark_jobs/silver/` → `data/silver/`

**Technology:** PySpark

Core transformation logic implemented in PySpark to build analytics-ready curated datasets.

### Silver Orders (`clean_orders.py`)

- Order deduplication
- Enrichment with restaurant city
- Payment mode correction
- Derived attributes — order hour, order value bucket

### Silver Delivery Timeline (`clean_delivery_events.py`)

- Pivoted lifecycle event reconstruction
- Rider assignment logic
- Rider wait time and travel duration metrics
- Delivery completion status flag

### Silver Order Facts (Primary Analytical Dataset)

- Integration of orders, delivery timelines, and refunds
- Actual vs promised delivery duration calculation
- SLA breach indicator
- Delivery success flag
- Rider assignment flag
- Refund aggregation

**Grain:** One row per order.

Output structure:

```
data/silver/
├── orders/
├── order_items/
├── delivery_events/
├── restaurants/
├── riders/
├── refunds/
└── support_tickets/
```

---

## Analytics Layer — dbt (In Progress)

**Location:** `dbt_project/`

**Technology:** dbt + DuckDB (`dbt-duckdb`)

Silver Parquet datasets are loaded into `data/warehouse/analytics.duckdb` via `src/loaders/silver_to_duckdb.py`. dbt connects to this DuckDB instance and handles all downstream transformation and modelling.

### Staging Models (`models/staging/`)

One model per silver source table. Responsibilities:

- Column renaming and type casting
- Lightweight filtering
- Establishing a clean contract for downstream models

Planned models:

- `stg_orders.sql`
- `stg_order_items.sql`
- `stg_delivery_events.sql`
- `stg_restaurants.sql`
- `stg_riders.sql`
- `stg_refunds.sql`
- `stg_support_tickets.sql`

### Intermediate Models (`models/intermediate/`)

Complex joins and business logic prior to mart materialization.

Planned models:

- `int_order_delivery_timeline.sql`
- `int_order_refund_joined.sql`
- `int_rider_order_metrics.sql`
- `int_restaurant_prep_times.sql`

### Mart Models (`models/marts/`)

**Core dimension and fact tables:**

- `dim_restaurants.sql`
- `dim_riders.sql`
- `dim_date.sql`
- `fct_orders.sql`

**Analytical marts answering the 5 business questions:**

| Mart | Business Question |
|---|---|
| `mart_sla_breach_analysis.sql` | Which cities and time slots have the highest SLA breach rate? |
| `mart_restaurant_prep_delays.sql` | Which restaurants cause prep-time delays pushing deliveries beyond target? |
| `mart_refund_drivers.sql` | What percentage of refunds are driven by delay, missing items, or cancellations? |
| `mart_rider_performance.sql` | Which riders consistently handle more orders without increasing late deliveries? |
| `mart_weekly_trends.sql` | How do completed orders, cancellations, and refund amounts trend week over week? |

### dbt Project Features

- **Tests:** `tests/assert_refund_pct_sums_to_100.sql` and schema tests on all staging models
- **Macros:** `datediff_minutes.sql`, `time_slot_bucket.sql`
- **Seeds:** `time_slots.csv` — reference table for time slot bucketing

---

## Data Flow Summary

```
src/generators/
      ↓
data/raw/          (CSV + JSONL)
      ↓
src/loaders/ [DuckDB]
      ↓
data/bronze/       (Parquet + metadata columns)
      ↓
src/profiling/     (quality validation)
      ↓
src/spark_jobs/silver/ [PySpark]
      ↓
data/silver/       (cleaned, enriched Parquet)
      ↓
src/loaders/silver_to_duckdb.py
      ↓
data/warehouse/analytics.duckdb
      ↓
dbt_project/ [dbt + DuckDB]
      ↓
staging → intermediate → marts
```

---

## Technology Stack

| Tool | Role |
|---|---|
| Python | Data generation, orchestration, profiling |
| DuckDB | Bronze ingestion, local warehouse, dbt backend |
| PySpark | Silver layer business transformations |
| dbt | Staging, intermediate, and mart modelling |
| Pandas | Profiling and exploratory validation |
| Git + GitHub | Version control and checkpoint commits |

---

## Repository Structure

```
food_del_pipeline/
├── config/                        # Central configuration
├── data/
│   ├── raw/                       # Generated source files
│   ├── bronze/                    # DuckDB-ingested Parquet
│   ├── silver/                    # PySpark-transformed Parquet
│   └── warehouse/analytics.duckdb # dbt-connected warehouse
├── src/
│   ├── generators/                # 7 dataset generators
│   ├── loaders/                   # Bronze ingestion + silver→DuckDB
│   ├── profiling/                 # Quality validation scripts
│   └── spark_jobs/silver/         # PySpark silver transforms
├── dbt_project/                   # dbt models, tests, macros, seeds
├── tests/                         # Python unit tests
├── notebooks/                     # Exploration and mart validation
└── docs/                          # Architecture and solution docs
```

---

## Expected Outcome

A complete analytics pipeline demonstrating practical implementation of modern data engineering principles — raw simulation, medallion layering, quality profiling, distributed transformation, and analytics modelling — producing actionable insights on delivery performance, operational efficiency, and refund behaviour.