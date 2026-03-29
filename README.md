# 🍔 Food Delivery Operations Analytics Pipeline

An end-to-end data engineering capstone project implementing a full analytics pipeline for a simulated food delivery platform, built using **Medallion Architecture** (Raw → Bronze → Silver → Analytics).

---

## 📌 Project Overview

This pipeline processes synthetic food delivery operational data across 8 Indian cities and 67 days of simulated operations to produce actionable business insights across five key questions:

1. Which cities and time slots have the highest SLA breach rates?
2. Which restaurants cause prep-time delays that push deliveries beyond target?
3. What percentage of refunds are driven by delay, missing items, or cancellations?
4. Which riders consistently handle more orders without increasing late deliveries?
5. How do completed orders, cancellations, and refund amounts trend week over week?

---

## 🏗️ Architecture

```
Raw Layer → Bronze Layer → Silver Layer → Analytics Layer (dbt)
```

| Layer | Technology | Status |
|-------|-----------|--------|
| Raw — Data Generation | Python (Faker + NumPy) | ✅ Complete |
| Bronze — Ingestion | DuckDB + Parquet | ✅ Complete |
| Profiling & Validation | Python (custom scripts) | ✅ Complete |
| Silver — Transformation | PySpark | ✅ Complete |
| Analytics — dbt Models | dbt + DuckDB | ✅ Complete |

---

## 🗂️ Repository Structure

```
food_del_pipeline/
├── config/                          # Central configuration (settings.py)
├── data/
│   ├── raw/                         # Generated source files (CSV + JSON)
│   ├── bronze/                      # DuckDB-ingested Parquet + metadata
│   ├── silver/                      # PySpark-cleaned, enriched Parquet
│   └── warehouse/analytics.duckdb  # dbt-connected local warehouse
├── src/
│   ├── generators/                  # 7 dataset generators
│   ├── loaders/                     # Bronze ingestion + silver→DuckDB loader
│   ├── profiling/                   # Quality validation scripts
│   └── spark_jobs/silver/           # PySpark silver transformation jobs
├── dbt_project/                     # dbt models, tests, macros, seeds
├── tests/                           # Python unit tests
├── notebooks/                       # Exploration and mart validation notebooks
├── logs/                            # Pipeline run logs
├── docs/                            # Architecture and solution docs
├── dbt_project.yml                  # dbt project configuration
├── pyproject.toml                   # Python project metadata
└── Makefile                         # Convenience commands
```

---

## ⚙️ Technology Stack

| Tool | Role |
|------|------|
| **Python** | Data generation, orchestration, profiling |
| **DuckDB** | Bronze ingestion, local analytical warehouse, dbt backend |
| **PySpark** | Silver layer business transformations |
| **dbt (dbt-duckdb)** | Staging, intermediate, and mart modelling |
| **Pandas** | Profiling and exploratory validation |
| **Jupyter Notebooks** | Data exploration and mart validation |
| **Git + GitHub** | Version control |

---

## 📦 Datasets

7 synthetically generated datasets simulating 67 days of food delivery operations across 8 cities:

| Dataset | Key Fields |
|---------|-----------|
| `orders.csv` | order_id, customer_id, restaurant_id, city, order_ts, promised_delivery_ts, status, order_value, payment_mode |
| `order_items.csv` | order_id, item_id, quantity, item_price, cuisine_type |
| `delivery_events.json` | order_id, rider_id, event_type, event_ts, latitude, longitude |
| `restaurants.csv` | restaurant_id, city, cuisine_type, rating_band, onboarding_date |
| `riders.csv` | rider_id, city, shift_type, joining_date |
| `refunds.csv` | refund_id, order_id, refund_ts, refund_reason, refund_amount |
| `support_tickets.csv` | ticket_id, order_id, ticket_type, created_ts, resolution_status |

Data includes realistic noise: missing values (2%), duplicates (0.5%), late-arriving events (3%), orphan keys (1%), and a 12% SLA breach rate.

---

## 🔄 Data Flow

```
src/generators/
      ↓
data/raw/              (CSV + JSON)
      ↓
src/loaders/ [DuckDB]
      ↓
data/bronze/           (Parquet + _source_file, _ingested_at, _batch_id)
      ↓
src/profiling/         (null checks, duplicate detection, lifecycle validation)
      ↓
src/spark_jobs/silver/ [PySpark]
      ↓
data/silver/           (cleaned, enriched Parquet + DQ flags)
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

## 🧱 dbt Layer

### Staging (`models/staging/`)
One model per silver source table — column renaming, type casting, and lightweight DQ filtering.

### Intermediate (`models/intermediate/`)
Complex joins and business logic — all `ephemeral` (no storage cost):

- `int_order_delivery_timeline` — pivots delivery events, computes prep/delivery/total times, sets SLA breach flag
- `int_order_refund_joined` — left joins orders to refunds, maps reason to driver category
- `int_rider_order_metrics` — daily per-rider: orders delivered, late count, avg delivery time
- `int_restaurant_prep_times` — per-restaurant prep statistics

### Marts (`models/marts/`)

**Core:**
`dim_restaurants`, `dim_riders`, `dim_date`, `fct_orders`

**Analytical Marts:**

| Mart | Business Question |
|------|-----------------|
| `mart_sla_breach_analysis` | Cities and time slots with highest SLA breach rate |
| `mart_restaurant_prep_delays` | Restaurants causing prep-time-driven late deliveries |
| `mart_refund_drivers` | Refund breakdown by delay, missing items, and cancellations |
| `mart_rider_performance` | Riders handling high volume without increasing lateness |
| `mart_weekly_trends` | Week-over-week order, cancellation, and refund trends |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.10+
- Java 8+ (required for PySpark)

### Installation

```bash
git clone https://github.com/TarjBaxi74/food_del_pipeline.git
cd food_del_pipeline
pip install -e .
```

### Run the Full Pipeline

```bash
python -m src.pipeline_runner
```

This executes all 5 steps in sequence:

| Step | Action |
|------|--------|
| Step 1 | Generate 7 raw datasets |
| Step 2 | Ingest raw files to Bronze (DuckDB → Parquet) |
| Step 3 | Run Silver transformations (PySpark) |
| Step 4 | Run dbt models and tests |
| Step 5 | Export analytical marts to CSV |

Typical runtime: ~35 seconds on a local machine.

### Run Individual Steps

```bash
# Generate raw data only
python -m src.generators.orchestrator

# Bronze ingestion only
python -m src.loaders.bronze_loader

# Silver transformations only
make run_spark

# Load silver to DuckDB
python -m src.loaders.silver_to_duckdb

# dbt only
cd dbt_project && dbt run && dbt test
```

---

## 🧪 Testing

### Python Tests

```bash
pytest tests/ -v
```

Tests cover generator output quality and silver transformation correctness.

### dbt Tests

Run automatically as part of the pipeline. Includes:

- `unique` + `not_null` schema tests on all staging models
- `not_null` tests on key mart columns
- Custom SQL test: `assert_refund_pct_sums_to_100` — validates that refund driver percentages sum to 100% (±0.1% tolerance)

---

## 📊 Output

After a successful pipeline run, five mart CSVs are exported to `data/warehouse/`:

| File | Contents |
|------|---------|
| `sla_breach_analysis.csv` | Breach rates by city and time slot |
| `restaurant_prep_delays.csv` | Prep time metrics and risk categories per restaurant |
| `refund_drivers.csv` | Refund count and amount by driver category |
| `rider_performance.csv` | Efficiency scores and performance tiers per rider |
| `weekly_trends.csv` | Week-over-week operational metrics |

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Full technical architecture — schemas, layer logic, dbt config, tech stack decisions |
| [`docs/SOLUTION.md`](docs/SOLUTION.md) | Business context, metric definitions, design decisions, DQ approach |
| [`reference.md`](reference.md) | High-level architecture reference |

---

## 🎯 Key Design Decisions

**Medallion Architecture** — Raw, Bronze, and Silver layers are kept separate so data quality issues are visible and auditable rather than silently fixed.

**DuckDB as warehouse backend** — Zero infrastructure, columnar OLAP performance, native Parquet support, and first-class dbt integration make it ideal for local pipeline development.

**PySpark for Silver** — Provides schema enforcement, partitioned output, and a production-representative transformation API even in local mode.

**Explicit DQ flags** — Every Silver table carries `_dq_*` columns rather than dropping bad records. Marts filter these out in the staging layer, maintaining a clean separation between visibility and consumption.

---

## 🔮 Future Enhancements

- Incremental dbt models to replace full-refresh processing
- Streaming ingestion with Kafka + Spark Structured Streaming
- Dashboard layer via Metabase or Apache Superset
- ML-based anomaly detection for data quality monitoring
- Airflow or Prefect for production scheduling and observability