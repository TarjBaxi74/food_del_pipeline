# Food Delivery Operations Analytics — Technical Architecture

## 1. System Overview

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FOOD DELIVERY ANALYTICS PIPELINE                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   GENERATE   │     │  RAW FILES   │     │   PYSPARK    │     │   DUCKDB     │
│   (Python)   │────►│  (CSV/JSON)  │────►│  (Bronze →   │────►│  (Warehouse) │
│              │     │              │     │   Silver)    │     │              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
  Faker + NumPy      data/raw/*.csv      data/bronze/        data/warehouse/
                     data/raw/*.json     data/silver/        analytics.duckdb
                                          (Parquet)
                                                                     │
                                                                     ▼
                                                            ┌──────────────┐
                                                            │     dbt      │
                                                            │  (Staging →  │
                                                            │   Marts)     │
                                                            └──────────────┘
```

### Data Flow

```
[1. Generate]   [2. Ingest]     [3. Clean]      [4. Model]      [5. Serve]
     │               │               │               │               │
     ▼               ▼               ▼               ▼               ▼
┌─────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌─────────┐
│ Python  │    │  Bronze  │    │  Silver  │    │   dbt    │    │  Marts  │
│Generators│──►│ Parquet  │───►│ Parquet  │───►│  Models  │───►│  Tables │
│         │    │ + Meta   │    │ + DQ     │    │          │    │         │
└─────────┘    └──────────┘    └──────────┘    └──────────┘    └─────────┘
     │               │               │               │               │
    raw/          bronze/          silver/         DuckDB          DuckDB
  *.csv           orders/          orders/         staging/        marts/
  *.json         events/          events/       intermediate/    analytics/
```

---

## 2. Repository Structure

```
food_del_pipeline/
├── pyproject.toml                     # Python dependencies
├── Makefile                           # Orchestration commands
├── .gitignore
│
├── config/
│   ├── __init__.py
│   └── settings.py                    # Central configuration
│
├── data/
│   ├── raw/                           # Generated source files
│   │   ├── orders.csv
│   │   ├── order_items.csv
│   │   ├── delivery_events.json
│   │   ├── restaurants.csv
│   │   ├── riders.csv
│   │   ├── refunds.csv
│   │   └── support_tickets.csv
│   ├── bronze/                        # Parquet + metadata columns
│   │   ├── orders/
│   │   ├── order_items/
│   │   ├── delivery_events/
│   │   ├── restaurants/
│   │   ├── riders/
│   │   ├── refunds/
│   │   └── support_tickets/
│   ├── silver/                        # Cleaned, enriched Parquet
│   │   └── [same structure as bronze]
│   └── warehouse/
│       └── analytics.duckdb           # dbt-connected warehouse
│
├── src/
│   ├── __init__.py
│   ├── generators/                    # Data generation
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── orders.py
│   │   ├── order_items.py
│   │   ├── delivery_events.py
│   │   ├── restaurants.py
│   │   ├── riders.py
│   │   ├── refunds.py
│   │   ├── support_tickets.py
│   │   └── orchestrator.py
│   │
│   ├── spark_jobs/
│   │   ├── __init__.py
│   │   ├── common/
│   │   │   ├── schemas.py
│   │   │   └── quality_checks.py
│   │   ├── bronze/
│   │   │   └── ingest_raw.py
│   │   ├── silver/
│   │   │   ├── clean_orders.py
│   │   │   └── clean_delivery_events.py
│   │   └── run_pipeline.py
│   │
│   └── loaders/
│       ├── __init__.py
│       └── silver_to_duckdb.py
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_delivery_events.sql
│   │   │   ├── stg_restaurants.sql
│   │   │   ├── stg_riders.sql
│   │   │   ├── stg_refunds.sql
│   │   │   └── stg_support_tickets.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_order_delivery_timeline.sql
│   │   │   ├── int_order_refund_joined.sql
│   │   │   ├── int_rider_order_metrics.sql
│   │   │   └── int_restaurant_prep_times.sql
│   │   │
│   │   └── marts/
│   │       ├── core/
│   │       │   ├── dim_restaurants.sql
│   │       │   ├── dim_riders.sql
│   │       │   ├── dim_date.sql
│   │       │   └── fct_orders.sql
│   │       └── analytics/
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
├── tests/
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
└── docs/
    ├── SOLUTION.md
    └── ARCHITECTURE.md
```

---

## 3. Data Schemas

### 3.1 Raw Layer

#### orders.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | Primary key — format: ORD-XXXXXXXXXXXX |
| customer_id | STRING | Yes | Customer identifier |
| restaurant_id | STRING | No | FK to restaurants |
| city | STRING | Yes | City name |
| order_ts | TIMESTAMP | No | Order placement time |
| promised_delivery_ts | TIMESTAMP | Yes | Promised delivery time |
| status | STRING | Yes | delivered / cancelled / in_progress |
| order_value | DOUBLE | Yes | Order amount in INR |
| payment_mode | STRING | Yes | UPI / Card / COD / Wallet |

#### delivery_events.json
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | FK to orders |
| rider_id | STRING | Yes | FK to riders (null pre-assignment) |
| event_type | STRING | No | See event sequence below |
| event_ts | TIMESTAMP | No | Event occurrence time |
| latitude | DOUBLE | Yes | GPS latitude |
| longitude | DOUBLE | Yes | GPS longitude |

**Event Sequence:**
```
order_confirmed → restaurant_accepted → food_prep_started → food_ready
    → rider_assigned → rider_picked_up → out_for_delivery → delivered
                                                          → delivery_failed
                                                          → cancelled
```

#### order_items.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | FK to orders |
| item_id | STRING | No | Item identifier |
| quantity | INTEGER | Yes | Quantity ordered |
| item_price | DOUBLE | Yes | Price per item |
| cuisine_type | STRING | Yes | Cuisine category |

#### restaurants.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| restaurant_id | STRING | No | Primary key |
| city | STRING | Yes | City location |
| cuisine_type | STRING | Yes | Primary cuisine |
| rating_band | STRING | Yes | A / B / C / D |
| onboarding_date | DATE | Yes | Date joined platform |

#### riders.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| rider_id | STRING | No | Primary key |
| city | STRING | Yes | Operating city |
| shift_type | STRING | Yes | morning / evening / night |
| joining_date | DATE | Yes | Date joined |

#### refunds.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| refund_id | STRING | No | Primary key |
| order_id | STRING | No | FK to orders |
| refund_ts | TIMESTAMP | Yes | Refund processing time |
| refund_reason | STRING | Yes | Reason category |
| refund_amount | DOUBLE | Yes | Amount in INR |

#### support_tickets.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| ticket_id | STRING | No | Primary key |
| order_id | STRING | No | FK to orders |
| ticket_type | STRING | Yes | Issue category |
| created_ts | TIMESTAMP | Yes | Ticket creation time |
| resolution_status | STRING | Yes | open / resolved / escalated |

---

### 3.2 Bronze Layer Additions

Every bronze table carries these metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| _source_file | STRING | Input file path |
| _ingested_at | TIMESTAMP | Ingestion timestamp |
| _batch_id | STRING | Processing batch identifier |
| _corrupt_record | STRING | Raw data if schema parsing failed |

---

### 3.3 Silver Layer Additions

Every silver table carries DQ flags:

| Column | Type | Description |
|--------|------|-------------|
| _dq_has_null_customer | BOOLEAN | Customer ID was null |
| _dq_invalid_order_value | BOOLEAN | Order value ≤ 0 or null |
| _dq_orphan_order | BOOLEAN | FK does not exist in parent table |
| _dq_invalid_sequence | BOOLEAN | Event arrived out of sequence |

**Derived columns added on orders:**

| Column | Type | Logic |
|--------|------|-------|
| order_date | DATE | Extracted from order_ts |
| order_hour | INTEGER | Hour component |
| time_slot | STRING | morning / lunch / evening / dinner |
| day_of_week | INTEGER | 1 = Sunday, 7 = Saturday |
| is_weekend | BOOLEAN | day_of_week IN (1, 7) |

---

## 4. Data Generation

### Configuration (`config/settings.py`)

```python
DATA_CONFIG = {
    "date_range": {
        "start": date(2026, 1, 15),
        "end":   date(2026, 3, 22),   # 67 days of operations
    },
    "volumes": {
        "orders_per_day_range": (800, 1500),
        "restaurants": 150,
        "riders": 300,
        "cities": [
            "Mumbai", "Delhi", "Bangalore", "Chennai",
            "Hyderabad", "Pune", "Kolkata", "Ahmedabad"
        ],
    },
    "defect_rates": {
        "null_rate":        0.02,
        "duplicate_rate":   0.005,
        "late_event_rate":  0.03,
        "orphan_rate":      0.01,
        "sla_breach_rate":  0.12,
    },
}

REFUND_REASONS = {
    "Delay":          0.35,
    "Missing_Items":  0.30,
    "Cancellation":   0.20,
    "Wrong_Order":    0.10,
    "Quality_Issue":  0.05,
}
```

### Generator Patterns

All generators extend `BaseGenerator` which provides `inject_nulls()` and `inject_duplicates()` helpers. Generators run in dependency order via `orchestrator.py`:

```
restaurants → riders → orders → order_items → delivery_events → refunds → support_tickets
```

Key realism mechanisms:
- **Peak-hour weighting**: Orders weighted toward lunch (12–13h) and dinner (19–20h)
- **Weekend uplift**: 30% higher order volume on Sat/Sun
- **SLA breach injection**: `_will_breach_sla` flag passed to delivery events generator to add extra delay on `food_ready` and `delivered` events
- **GPS jitter**: City-anchored coordinates with ~2 km random offset per event
- **Refund amounts**: Percentage of order value varies by reason type (e.g. full refund for cancellations, partial for delays)

---

## 5. Spark Jobs

### Session Configuration

```python
SparkSession.builder
    .appName("FoodDeliveryPipeline")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
```

### Bronze Ingestion (`src/spark_jobs/bronze/ingest_raw.py`)

`BronzeIngestion` reads raw files with PERMISSIVE mode (corrupt records captured rather than dropped), appends `_source_file`, `_ingested_at`, and `_batch_id`, and writes Parquet to `data/bronze/`.

### Silver Transformations

#### `clean_orders.py` — `OrdersCleaner`

Transformation chain:

```
_remove_corrupt_records
    → _deduplicate          (keep latest per order_id by _ingested_at)
    → _standardize_fields   (UPPER + TRIM on city, status, payment_mode)
    → _handle_nulls         (payment_mode → 'UNKNOWN'; negative order_value → NULL)
    → _add_derived_columns  (order_date, order_hour, time_slot, day_of_week, is_weekend)
    → _add_data_quality_flags
```

Output partitioned by `order_date`.

#### `clean_delivery_events.py`

Pivots raw event rows into a single row per order with timestamped event columns, assigns rider, computes `prep_time_minutes`, `delivery_time_minutes`, `total_time_minutes`, and sets `is_sla_breached`.

#### Silver Order Facts

Primary analytical grain — one row per order — joining orders, delivery timeline, and refunds. Carries SLA breach flag, delivery success flag, and refund aggregation.

---

## 6. dbt Layer

### Project Config (`dbt_project.yml`)

```yaml
models:
  food_delivery_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      core:
        +materialized: table
        +schema: marts
      analytics:
        +materialized: table
        +schema: analytics
```

### Staging

One model per silver source. Responsibilities: column renaming, type casting, lightweight DQ filtering. Sources declared in `_staging.yml` pointing to `silver` schema in `analytics.duckdb`.

### Intermediate

| Model | Purpose |
|-------|---------|
| `int_order_delivery_timeline` | Pivots delivery events per order; computes prep, delivery, and total durations; sets `is_sla_breached` |
| `int_order_refund_joined` | Left joins orders to refunds; maps raw reason to driver category |
| `int_rider_order_metrics` | Daily per-rider: orders delivered, late count, avg delivery time |
| `int_restaurant_prep_times` | Per-restaurant prep statistics aggregated from delivery timeline |

All intermediate models are `ephemeral` — no DuckDB materialisation, no storage cost.

### Marts

#### Core

| Model | Description |
|-------|-------------|
| `dim_restaurants` | Restaurant dimension with city, cuisine, rating |
| `dim_riders` | Rider dimension with city, shift type, tenure |
| `dim_date` | Date spine for period-based analysis |
| `fct_orders` | One row per order with all measures and flags |

#### Analytical Marts

| Mart | Business Question |
|------|------------------|
| `mart_sla_breach_analysis` | Which cities and time slots have the highest SLA breach rate? |
| `mart_restaurant_prep_delays` | Which restaurants cause prep-time-driven late deliveries? |
| `mart_refund_drivers` | What % of refunds are driven by delay, missing items, cancellations? |
| `mart_rider_performance` | Which riders handle high volume without increasing lateness? |
| `mart_weekly_trends` | How do orders, cancellations, and refund amounts trend week over week? |

### Macros

```sql
-- datediff_minutes.sql
{% macro datediff_minutes(start_col, end_col) %}
    (epoch({{ end_col }}) - epoch({{ start_col }})) / 60
{% endmacro %}

-- time_slot_bucket.sql
{% macro time_slot_bucket(hour_col) %}
    case
        when {{ hour_col }} between 8  and 11 then 'morning'
        when {{ hour_col }} between 12 and 15 then 'lunch'
        when {{ hour_col }} between 16 and 19 then 'evening'
        when {{ hour_col }} between 20 and 23 then 'dinner'
        else 'late_night'
    end
{% endmacro %}
```

### Tests

| Test | Type | Target |
|------|------|--------|
| `unique` + `not_null` on `order_id` | Schema | `stg_orders` |
| `unique` + `not_null` on `restaurant_id` | Schema | `stg_restaurants` |
| `not_null` on key mart columns | Schema | All mart models |
| `assert_refund_pct_sums_to_100` | Custom SQL | `mart_refund_drivers` |

---

## 7. Technology Stack

| Tool | Version | Role |
|------|---------|------|
| Python | 3.11 | Data generation, orchestration, profiling |
| PySpark | 3.5+ | Bronze ingestion, silver transformation |
| DuckDB | 0.10+ | Local analytical warehouse, dbt backend |
| dbt-duckdb | 1.7+ | Staging, intermediate, and mart modelling |
| Pandas | 2.0+ | Profiling and exploratory validation |
| Faker | 20.0+ | Synthetic data generation |
| Jupyter | — | Exploration and mart validation |

### Why DuckDB over Postgres / Spark SQL for the warehouse?

| Factor | DuckDB | Postgres | Spark SQL |
|--------|--------|----------|-----------|
| Setup | Zero-config, embedded | Requires server | Requires cluster |
| OLAP performance | Columnar, excellent | OLTP-focused | Good at scale |
| Native Parquet | Yes | Via extensions | Yes |
| dbt support | dbt-duckdb | dbt-postgres | Limited |
| Local dev | Ideal | Overhead | JVM overhead |

---

## 8. Orchestration

Full pipeline runs via a single entry point:

```bash
python -m src.pipeline_runner
```

Steps executed in order:
1. Raw data generation
2. Bronze ingestion (DuckDB → Parquet)
3. Silver transformations (PySpark)
4. dbt model run + tests
5. Warehouse export (marts → CSV)

Convenience targets also available via `Makefile`.

---

## 9. Configuration Reference

### `pyproject.toml`

```toml
[project]
name = "food-del-pipeline"
version = "0.1.0"
description = "Local Food Delivery Analytics Pipeline"
requires-python = ">=3.10"
dependencies = [
    "pyspark>=3.5.0",
    "duckdb>=0.10.0",
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "faker>=20.0.0",
    "dbt-duckdb>=1.7.0",
    "pytest>=7.0.0",
]
```

### `.gitignore` highlights

```
data/raw/
data/bronze/
data/silver/
data/warehouse/
dbt_project/target/
dbt_project/dbt_packages/
__pycache__/
.venv/
```