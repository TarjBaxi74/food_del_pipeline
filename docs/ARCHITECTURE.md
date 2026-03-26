# Food Delivery Operations Analytics — Technical Architecture

## 1. System Overview

### Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        FOOD DELIVERY ANALYTICS PIPELINE                          │
└──────────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   GENERATE   │    │  RAW FILES   │    │   DUCKDB     │    │   PYSPARK    │
│   (Python)   │───►│  (CSV/JSON)  │───►│  (Bronze     │───►│  (Silver     │
│  + Faker     │    │              │    │  Ingestion)  │    │  Transform)  │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
       │                   │                   │                    │
       ▼                   ▼                   ▼                    ▼
  src/generators/    data/raw/           data/bronze/         data/silver/
                     *.csv               (Parquet +           (Cleaned
                     *.json              metadata)            Parquet)
                                              │                    │
                                              ▼                    │
                                    src/profiling/                 │
                                    (Quality Checks)               │
                                                                   ▼
                                                        src/loaders/
                                                        silver_to_duckdb.py
                                                                   │
                                                                   ▼
                                                        ┌──────────────────┐
                                                        │   data/warehouse │
                                                        │  analytics.duckdb│
                                                        └──────────────────┘
                                                                   │
                                                                   ▼
                                                        ┌──────────────────┐
                                                        │    dbt_project/  │
                                                        │  staging →       │
                                                        │  intermediate →  │
                                                        │  marts           │
                                                        └──────────────────┘
```

### Data Flow

```
[1. Generate]    [2. Ingest]       [3. Profile]     [4. Transform]   [5. Load]     [6. Model]
     │                │                 │                 │               │              │
     ▼                ▼                 ▼                 ▼               ▼              ▼
┌─────────┐    ┌──────────┐      ┌──────────┐     ┌──────────┐    ┌──────────┐   ┌──────────┐
│ Python  │    │  Bronze  │      │ Profiling│     │  Silver  │    │  DuckDB  │   │   dbt    │
│Generators│──►│ DuckDB + │─────►│ Scripts  │────►│ PySpark  │───►│Warehouse │──►│  Models  │
│         │    │ Parquet  │      │          │     │ Parquet  │    │          │   │  + Marts │
└─────────┘    └──────────┘      └──────────┘     └──────────┘    └──────────┘   └──────────┘
     │               │                 │                 │               │              │
   raw/           bronze/           logs/            silver/         analytics     staging/
  *.csv          orders/           quality          orders/          .duckdb     intermediate/
  *.json        events/            reports          events/                        marts/
```

---

## 2. Project Structure

```
food_del_pipeline/
├── pyproject.toml                     # Python project dependencies
├── Makefile                           # Pipeline orchestration
├── dbt_project.yml                    # Root dbt configuration
├── dev.duckdb                         # Development DuckDB instance
├── reference.md                       # Architecture reference (working doc)
├── .gitignore
│
├── config/
│   └── settings.py                    # Central configuration (paths, params)
│
├── data/
│   ├── raw/                           # Python-generated source files
│   │   ├── orders.csv
│   │   ├── order_items.csv
│   │   ├── delivery_events.json       # JSONL format
│   │   ├── restaurants.csv
│   │   ├── riders.csv
│   │   ├── refunds.csv
│   │   └── support_tickets.csv
│   ├── bronze/                        # DuckDB-ingested Parquet (raw + metadata)
│   │   ├── orders/
│   │   ├── order_items/
│   │   ├── delivery_events/
│   │   ├── restaurants/
│   │   ├── riders/
│   │   ├── refunds/
│   │   └── support_tickets/
│   ├── silver/                        # PySpark-transformed Parquet (clean + DQ flags)
│   │   ├── orders/
│   │   ├── order_items/
│   │   ├── delivery_events/
│   │   ├── restaurants/
│   │   ├── riders/
│   │   ├── refunds/
│   │   └── support_tickets/
│   └── warehouse/
│       └── analytics.duckdb           # dbt-connected analytical warehouse
│
├── src/
│   ├── generators/                    # Synthetic data generation
│   │   ├── __init__.py
│   │   ├── base.py                    # Base generator class
│   │   ├── orders.py
│   │   ├── order_items.py
│   │   ├── delivery_events.py
│   │   ├── restaurants.py
│   │   ├── riders.py
│   │   ├── refunds.py
│   │   ├── support_tickets.py
│   │   └── orchestrator.py            # Coordinates all generator runs in order
│   │
│   ├── loaders/                       # Ingestion and loading
│   │   ├── __init__.py
│   │   ├── bronze_ingest.py           # DuckDB Bronze ingestion
│   │   └── silver_to_duckdb.py        # Load Silver Parquet → DuckDB warehouse
│   │
│   ├── profiling/                     # Data quality validation scripts
│   │   ├── __init__.py
│   │   └── profile_bronze.py          # Null %, duplicates, orphans, lifecycle checks
│   │
│   └── spark_jobs/
│       ├── __init__.py
│       ├── silver/
│       │   ├── __init__.py
│       │   ├── clean_orders.py        # Order dedup, enrichment, derived columns
│       │   └── clean_delivery_events.py  # Event pivot, rider metrics, completion flags
│       ├── run_pipeline.py            # Spark job orchestrator (entry point)
│       └── test_spark.py              # Spark connectivity test
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                   # DuckDB connection config
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging.yml           # Source + model YAML with schema tests
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_delivery_events.sql
│   │   │   ├── stg_restaurants.sql
│   │   │   ├── stg_riders.sql
│   │   │   ├── stg_refunds.sql
│   │   │   └── stg_support_tickets.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── _intermediate.yml
│   │   │   ├── int_order_delivery_timeline.sql
│   │   │   ├── int_order_refund_joined.sql
│   │   │   ├── int_rider_order_metrics.sql
│   │   │   └── int_restaurant_prep_times.sql
│   │   │
│   │   └── marts/
│   │       ├── core/
│   │       │   ├── _core.yml
│   │       │   ├── dim_restaurants.sql
│   │       │   ├── dim_riders.sql
│   │       │   ├── dim_date.sql
│   │       │   └── fct_orders.sql
│   │       └── analytics/
│   │           ├── _analytics.yml
│   │           ├── mart_sla_breach_analysis.sql
│   │           ├── mart_restaurant_prep_delays.sql
│   │           ├── mart_refund_drivers.sql
│   │           ├── mart_rider_performance.sql
│   │           └── mart_weekly_trends.sql
│   │
│   ├── tests/
│   │   └── assert_refund_pct_sums_to_100.sql
│   │
│   ├── macros/
│   │   ├── datediff_minutes.sql
│   │   └── time_slot_bucket.sql
│   │
│   └── seeds/
│       └── time_slots.csv
│
├── tests/                             # Python unit tests
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_generators/
│   │   └── test_data_quality.py
│   └── test_spark_jobs/
│       └── test_silver_transforms.py
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   └── 02_validate_marts.ipynb
│
├── logs/                              # Pipeline and profiling output logs
│
└── docs/
    ├── SOLUTION.md
    └── ARCHITECTURE.md
```

---

## 3. Data Schemas

### 3.1 Raw Layer Schemas

#### orders.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | Primary key, format: ORD-XXXXXXXXXXXX |
| customer_id | STRING | Yes (~2% null) | Customer identifier |
| restaurant_id | STRING | No | FK to restaurants |
| city | STRING | Yes | City name |
| order_ts | TIMESTAMP | No | Order placement timestamp |
| promised_delivery_ts | TIMESTAMP | Yes | Promised delivery time |
| status | STRING | Yes | `delivered`, `cancelled`, `in_progress` |
| order_value | DOUBLE | Yes | Order amount in INR |
| payment_mode | STRING | Yes (some missing) | `UPI`, `Card`, `COD`, `Wallet` |

#### delivery_events.json (JSONL)
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | FK to orders |
| rider_id | STRING | Yes | FK to riders (null for pre-assignment events) |
| event_type | STRING | No | Event name (see sequence below) |
| event_ts | TIMESTAMP | No | Actual event occurrence time |
| latitude | DOUBLE | Yes | GPS latitude at event time |
| longitude | DOUBLE | Yes | GPS longitude at event time |

**Delivery Event Sequence**:
```
1. order_confirmed
2. restaurant_accepted
3. food_prep_started
4. food_ready
5. rider_assigned
6. rider_picked_up
7. out_for_delivery
8. delivered  ──┐
   delivery_failed  ├── Terminal events (exactly one ends each lifecycle)
   cancelled   ──┘
```

#### order_items.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | FK to orders |
| item_id | STRING | No | Item identifier |
| quantity | INTEGER | Yes | Quantity ordered |
| item_price | DOUBLE | Yes | Price per unit in INR |
| cuisine_type | STRING | Yes | Cuisine category |

#### restaurants.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| restaurant_id | STRING | No | Primary key |
| city | STRING | Yes | City of operation |
| cuisine_type | STRING | Yes | Primary cuisine served |
| rating_band | STRING | Yes | A, B, C, or D |
| onboarding_date | DATE | Yes | Date joined the platform |

#### riders.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| rider_id | STRING | No | Primary key |
| city | STRING | Yes | Operating city |
| shift_type | STRING | Yes | `morning`, `evening`, `night` |
| joining_date | DATE | Yes | Date joined the fleet |

#### refunds.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| refund_id | STRING | No | Primary key |
| order_id | STRING | No | FK to orders |
| refund_ts | TIMESTAMP | Yes | Refund processing timestamp |
| refund_reason | STRING | Yes | `Delay`, `Missing_Items`, `Wrong_Order`, `Cancellation`, `Quality_Issue` |
| refund_amount | DOUBLE | Yes | Refund amount in INR |

#### support_tickets.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| ticket_id | STRING | No | Primary key |
| order_id | STRING | No | FK to orders |
| ticket_type | STRING | Yes | Issue category |
| created_ts | TIMESTAMP | Yes | Ticket creation timestamp |
| resolution_status | STRING | Yes | `open`, `resolved`, `escalated` |

---

### 3.2 Bronze Layer Additions

Every Bronze table is written to Parquet and enriched with these metadata columns during DuckDB ingestion:

| Column | Type | Description |
|--------|------|-------------|
| `_source_file` | STRING | Input file path |
| `_ingested_at` | TIMESTAMP | Ingestion timestamp (wall clock) |
| `_load_date` | DATE | Date of the ingestion run |

---

### 3.3 Silver Layer Additions

Every Silver table inherits Bronze metadata and adds data quality flag columns:

| Column | Type | Description |
|--------|------|-------------|
| `_dq_has_null_customer` | BOOLEAN | customer_id is null (orders table) |
| `_dq_invalid_order_value` | BOOLEAN | order_value is null or ≤ 0 |
| `_dq_orphan_order` | BOOLEAN | order_id has no match in the orders master |
| `_dq_invalid_sequence` | BOOLEAN | delivery event timestamps are out of expected order |
| `_dq_missing_payment_mode` | BOOLEAN | payment_mode is null after imputation attempt |
| `_is_duplicate` | BOOLEAN | Record is a duplicate of an earlier row for the same order_id |

**Silver Orders** additionally includes these derived columns:

| Column | Description |
|--------|-------------|
| `order_hour` | Hour extracted from order_ts |
| `order_date` | Date extracted from order_ts |
| `order_value_bucket` | `low` / `medium` / `high` derived from order_value |
| `restaurant_city` | City joined from restaurants master |
| `payment_mode_corrected` | Imputed or corrected payment_mode value |

**Silver Delivery Events** additionally includes these pivoted and derived columns:

| Column | Description |
|--------|-------------|
| `order_confirmed_ts` | Timestamp pivoted from event_type = 'order_confirmed' |
| `restaurant_accepted_ts` | Timestamp pivoted from event_type = 'restaurant_accepted' |
| `food_ready_ts` | Timestamp pivoted from event_type = 'food_ready' |
| `rider_assigned_ts` | Timestamp pivoted from event_type = 'rider_assigned' |
| `rider_picked_up_ts` | Timestamp pivoted from event_type = 'rider_picked_up' |
| `delivered_ts` | Timestamp pivoted from event_type = 'delivered' |
| `prep_time_minutes` | food_ready_ts − restaurant_accepted_ts (minutes) |
| `rider_wait_time_minutes` | rider_picked_up_ts − rider_assigned_ts (minutes) |
| `travel_time_minutes` | delivered_ts − rider_picked_up_ts (minutes) |
| `delivery_completed` | Boolean: terminal event is 'delivered' |
| `rider_assigned_flag` | Boolean: rider_assigned event is present |

---

## 4. Component Details

### 4.1 Data Generators (`src/generators/`)

All generators inherit from `base.py`, which provides a shared Faker instance, common reference data (cities, cuisine types, order statuses), a configurable 60-day date range, seeded random state for reproducibility, and defect injection rates controlled via `config/settings.py`.

The orchestrator (`orchestrator.py`) runs all generators in dependency order — restaurants and riders are produced first, followed by orders, then order_items and delivery_events, and finally refunds and support_tickets which depend on order outcomes.

| Generator | Key Behaviours |
|-----------|----------------|
| `orders.py` | Peak-hour demand weighting for lunch and dinner, multi-city distribution, realistic status split across delivered / cancelled / in_progress |
| `delivery_events.py` | Full 8-event lifecycle per order, GPS coordinates per event, injected late events (~3% of records), occasional orphan order references (~1%) |
| `refunds.py` | Generated only for delivered or cancelled orders, weighted reason distribution across delay / missing items / cancellation / quality |
| `support_tickets.py` | Correlated with refund and cancellation events; resolution status distributed across open / resolved / escalated |

---

### 4.2 Bronze Ingestion (`src/loaders/bronze_ingest.py`)

**Technology**: DuckDB

DuckDB reads each raw file using its native CSV and JSON ingestion functions, appends the three metadata columns, and writes the result to partitioned Parquet under `data/bronze/`. Schema normalization is applied at this stage to enforce consistent column types across all source files. No business logic runs in the Bronze layer — raw data is preserved as-is with only structural standardization and metadata enrichment applied.

---

### 4.3 Profiling Layer (`src/profiling/profile_bronze.py`)

Runs after Bronze ingestion completes and before Silver transformation begins. Results are written to `logs/` as quality reports.

| Check | Datasets |
|-------|----------|
| Null percentage per column | All Bronze tables |
| Duplicate order_id detection | bronze/orders |
| Orphan refunds (no matching order_id) | bronze/refunds |
| Orphan delivery events (no matching order_id) | bronze/delivery_events |
| Delivery event lifecycle completeness | bronze/delivery_events |
| Delay and SLA distribution summary | bronze/orders + bronze/delivery_events |

The profiling layer does not modify data. Its sole purpose is to surface quality characteristics before transformation logic runs, enabling engineers to confirm expected defect rates and catch unexpected upstream issues.

---

### 4.4 Silver Transformations (`src/spark_jobs/silver/`)

**Technology**: PySpark

#### clean_orders.py

Handles order deduplication using a window function ordered by `order_ts` per `order_id`; enrichment with restaurant city via a left join to the restaurants Bronze table; payment mode imputation for null values; derivation of `order_hour`, `order_date`, and `order_value_bucket`; and population of all `_dq_*` flag columns. The `_is_duplicate` flag is set to true on all rows beyond the first occurrence per order_id, preserving the audit trail while allowing downstream models to filter them out cleanly.

#### clean_delivery_events.py

Handles pivoting of the 8-event lifecycle into a single row per order using groupBy, pivot, and min aggregation on event_ts; computation of `prep_time_minutes`, `rider_wait_time_minutes`, and `travel_time_minutes` from the pivoted timestamps; setting of `delivery_completed` and `rider_assigned_flag` booleans; and flagging of records where food_ready_ts precedes restaurant_accepted_ts as `_dq_invalid_sequence`.

Both Silver jobs write output to `data/silver/` as Parquet using a full overwrite on each run.

---

### 4.5 Silver → DuckDB Loader (`src/loaders/silver_to_duckdb.py`)

Reads all seven Silver Parquet directories and registers each as a table in `data/warehouse/analytics.duckdb` using DuckDB's Parquet glob-read capability. This makes Silver data available as SQL sources for dbt without requiring any Spark context at the modelling stage. Each table is recreated using `CREATE OR REPLACE TABLE` on every run, making the load step idempotent.

---

## 5. dbt Layer Details

### 5.1 dbt Project Configuration

The project is named `dbt_project` and uses the `dbt-duckdb` adapter connecting to `data/warehouse/analytics.duckdb`. All model paths, test paths, seed paths, and macro paths point into the `dbt_project/` subdirectory. Staging and intermediate models are materialised as views; core dimension/fact tables and analytics mart models are materialised as tables.

---

### 5.2 Staging Models (`models/staging/`)

One staging model per Silver source table. Each model selects from the corresponding Silver source, renames columns for consistency, applies explicit type casts, and filters out duplicate rows and records flagged by `_dq_*` columns. No aggregation or business logic is applied at the staging layer — its only job is to expose a clean, well-typed contract to downstream models.

| Model | Source Table | Key Filters Applied |
|-------|-------------|---------------------|
| `stg_orders.sql` | silver.orders | Exclude duplicates, exclude invalid order_value |
| `stg_order_items.sql` | silver.order_items | Exclude null order_id |
| `stg_delivery_events.sql` | silver.delivery_events | Exclude orphan orders, exclude invalid sequences |
| `stg_restaurants.sql` | silver.restaurants | No DQ exclusions (master data) |
| `stg_riders.sql` | silver.riders | No DQ exclusions (master data) |
| `stg_refunds.sql` | silver.refunds | Exclude orphan orders |
| `stg_support_tickets.sql` | silver.support_tickets | Exclude orphan orders |

---

### 5.3 Intermediate Models (`models/intermediate/`)

Intermediate models handle complex joins and multi-step business logic that would make mart models unwieldy if written directly against staging tables.

| Model | Purpose |
|-------|---------|
| `int_order_delivery_timeline.sql` | Joins stg_orders with stg_delivery_events; computes the SLA breach flag, total delivery duration in minutes, and time slot bucket for each order |
| `int_order_refund_joined.sql` | Left-joins stg_orders with stg_refunds; makes refund amount and driver category available at the order grain for mart consumption |
| `int_rider_order_metrics.sql` | Aggregates delivery events and SLA breach status per rider; produces total deliveries, late deliveries, on-time rate, and orders per day |
| `int_restaurant_prep_times.sql` | Aggregates prep time per restaurant; computes average prep time, delay rate percentage, and risk category (High / Medium / Low) |

---

### 5.4 Mart Models (`models/marts/`)

#### Core Dimension and Fact Tables

| Model | Description |
|-------|-------------|
| `dim_restaurants.sql` | Restaurant dimension with city, cuisine_type, rating_band, and onboarding_date |
| `dim_riders.sql` | Rider dimension with city, shift_type, and joining_date |
| `dim_date.sql` | Date spine covering the full 60-day simulation period |
| `fct_orders.sql` | Primary fact table at order grain; integrates orders, delivery timeline, and refunds into a single wide table with all key metrics and flags |

#### Analytics Marts

| Mart | Business Question Answered |
|------|---------------------------|
| `mart_sla_breach_analysis.sql` | Which cities and time slots have the highest SLA breach rate? — Breach rate % by city and time slot, with within-city ranking |
| `mart_restaurant_prep_delays.sql` | Which restaurants cause prep-time delays? — Average prep time, delay rate %, and risk category per restaurant |
| `mart_refund_drivers.sql` | What percentage of refunds are driven by delay, missing items, or cancellations? — Refund count and % share by driver category |
| `mart_rider_performance.sql` | Which riders handle more orders without increasing late deliveries? — On-time rate, efficiency score, and performance rank per rider within city |
| `mart_weekly_trends.sql` | How do completed orders, cancellations, and refunds trend week over week? — Weekly GMV, cancellation rate, refund rate, and WoW % changes |

---

### 5.5 dbt Macros (`macros/`)

| Macro | Purpose |
|-------|---------|
| `datediff_minutes(start_col, end_col)` | Computes the difference between two timestamp columns in minutes using DuckDB's epoch function; used consistently across intermediate and mart models |
| `time_slot_bucket(hour_col)` | Classifies an integer hour into one of five named time slots: morning, lunch, evening, dinner, or late_night; ensures consistent bucketing across all models |

---

### 5.6 Seeds (`seeds/`)

`time_slots.csv` provides a reference table mapping hour ranges to time slot names. It is used as a join target in the SLA breach analysis mart to ensure consistent slot labels across all models and avoids repeating the bucketing logic inline.

---

## 6. Testing Strategy

### 6.1 dbt Schema Tests

Every staging model has schema tests defined in `_staging.yml`. Primary key columns are tested for uniqueness and non-nullness. Foreign key columns are tested with relationship tests against their corresponding dimension staging models. Critical metric fields such as order_value and refund_amount are tested for non-null values.

### 6.2 Custom dbt Test

`tests/assert_refund_pct_sums_to_100.sql` verifies that the sum of all `refund_count_pct` values in `mart_refund_drivers` equals 100% within a 0.1% rounding tolerance. This guards against category mapping gaps that would cause percentages to under-sum silently.

### 6.3 Python Unit Tests (`tests/`)

| Test File | Scope |
|-----------|-------|
| `test_generators/test_data_quality.py` | Validates generator output — duplicate rate within expected bounds, order values positive, status values within the allowed set |
| `test_spark_jobs/test_silver_transforms.py` | Validates Silver transform logic — deduplication removes the correct rows, DQ flags are set on expected records, pivoted timestamps populate correctly |

---

## 7. Orchestration

### Makefile

The Makefile defines one target per pipeline stage, plus `all` to run the full pipeline end-to-end.

| Target | Action |
|--------|--------|
| `generate` | Run all 7 data generators via orchestrator |
| `bronze` | DuckDB Bronze ingestion (raw CSV/JSON → Parquet + metadata) |
| `profile` | Run Bronze profiling and quality validation checks |
| `silver` | PySpark Silver transformations |
| `load` | Load Silver Parquet into DuckDB warehouse |
| `dbt` | Run `dbt run` then `dbt test` |
| `test` | Run all Python unit tests via pytest |
| `clean` | Remove all generated data and dbt artefacts |
| `setup` | Install Python dependencies and dbt packages |
| `all` | Run full pipeline: generate → bronze → profile → silver → load → dbt |

---

## 8. Configuration Reference

### pyproject.toml

Declares the project as `food_del_pipeline` version 0.1.0, requiring Python ≥ 3.10. Key dependencies are pyspark ≥ 3.5.0, duckdb ≥ 0.10.0, pandas ≥ 2.0.0, numpy ≥ 1.24.0, faker ≥ 20.0.0, dbt-duckdb ≥ 1.7.0, and pytest ≥ 7.0.0. The build backend is setuptools.

### .gitignore

The following are excluded from version control since they contain generated or derived data: `data/raw/`, `data/bronze/`, `data/silver/`, `data/warehouse/`, `logs/`, `dbt_project/target/`, `dbt_project/dbt_packages/`, `dbt_project/logs/`, and standard Python artefacts including `__pycache__/`, `.venv/`, and `.egg-info/`.

---

## 9. Technology Stack Summary

| Tool | Version | Role |
|------|---------|------|
| Python | ≥ 3.10 | Data generation, orchestration, profiling |
| Faker | ≥ 20.0 | Synthetic data generation |
| DuckDB | ≥ 0.10 | Bronze ingestion, local warehouse, dbt backend |
| PySpark | ≥ 3.5 | Silver layer business transformations |
| dbt-duckdb | ≥ 1.7 | Staging, intermediate, and mart modelling |
| Pandas | ≥ 2.0 | Profiling, exploration, validation |
| pytest | ≥ 7.0 | Python unit testing |
| Make | — | Pipeline orchestration |
| Git + GitHub | — | Version control and checkpoints |