# 🍔 Food Delivery Operations Analytics Pipeline

A capstone data engineering project implementing an end-to-end analytics pipeline for a simulated food delivery platform, built using **Medallion Architecture** (Raw → Bronze → Silver → Analytics).

---

## 📌 Project Overview

This pipeline processes simulated food delivery operational data to generate actionable insights across five key business questions:

- Which cities and time slots have the highest SLA breach rates?
- Which restaurants cause prep-time delays that push deliveries beyond target?
- What percentage of refunds are driven by delay, missing items, or cancellations?
- Which riders consistently handle more orders without increasing late deliveries?
- How do completed orders, cancellations, and refund amounts trend week over week?

---

## 🏗️ Architecture

The pipeline follows a **Medallion (Lakehouse) Architecture**:

```
Raw Layer → Bronze Layer → Silver Layer → Analytics Layer (dbt)
```

| Layer | Technology | Status |
|---|---|---|
| Raw (Data Generation) | Python | ✅ Complete |
| Bronze (Ingestion) | DuckDB + Parquet | ✅ Complete |
| Profiling & Validation | Python (custom scripts) | ✅ Complete |
| Silver (Transformation) | PySpark | ✅ Complete |
| Analytics (dbt models) | dbt + DuckDB | ✅ Complete |

---

## 🗂️ Repository Structure

```
food_del_pipeline/
├── config/                          # Central configuration
├── data/
│   ├── raw/                         # Generated source files (CSV + JSONL)
│   ├── bronze/                      # DuckDB-ingested Parquet files
│   ├── silver/                      # PySpark-transformed Parquet files
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
├── Makefile                         # Convenience commands
└── reference.md                     # Detailed architecture reference
```

---

## ⚙️ Technology Stack

| Tool | Role |
|---|---|
| **Python** | Data generation, orchestration, profiling |
| **DuckDB** | Bronze ingestion, local analytical warehouse, dbt backend |
| **PySpark** | Silver layer business transformations |
| **dbt (dbt-duckdb)** | Staging, intermediate, and mart modelling |
| **Pandas** | Profiling and exploratory validation |
| **Jupyter Notebooks** | Exploration and mart validation |
| **SQL** | dbt model queries and DuckDB analytics |
| **Git + GitHub** | Version control |

---

## 📦 Datasets

The pipeline uses 7 synthetically generated datasets simulating 60+ days of food delivery operations:

| Dataset | Key Fields |
|---|---|
| `orders.csv` | order_id, customer_id, restaurant_id, city, order_ts, promised_delivery_ts, status, order_value, payment_mode |
| `order_items.csv` | order_id, item_id, quantity, item_price, cuisine_type |
| `delivery_events.json` | order_id, rider_id, event_type, event_ts, latitude, longitude |
| `restaurants.csv` | restaurant_id, city, cuisine_type, rating_band, onboarding_date |
| `riders.csv` | rider_id, city, shift_type, joining_date |
| `refunds.csv` | refund_id, order_id, refund_ts, refund_reason, refund_amount |
| `support_tickets.csv` | ticket_id, order_id, ticket_type, created_ts, resolution_status |

Data includes realistic noise: missing values, duplicates, late events, cancellations, and mismatched keys.

---

## 🔄 Data Flow

```
src/generators/
      ↓
data/raw/              (CSV + JSONL)
      ↓
src/loaders/ [DuckDB]
      ↓
data/bronze/           (Parquet + metadata columns)
      ↓
src/profiling/         (null checks, duplicate detection, lifecycle validation)
      ↓
src/spark_jobs/silver/ [PySpark]
      ↓
data/silver/           (cleaned, enriched Parquet)
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

## 🧱 dbt Layer (Analytics)

### Staging (`models/staging/`)
One model per silver source — column renaming, type casting, and lightweight filtering.

### Intermediate (`models/intermediate/`)
Complex joins and business logic:
- `int_order_delivery_timeline.sql`
- `int_order_refund_joined.sql`
- `int_rider_order_metrics.sql`
- `int_restaurant_prep_times.sql`

### Marts (`models/marts/`)

**Dimensions & Facts:**
- `dim_restaurants.sql`, `dim_riders.sql`, `dim_date.sql`, `fct_orders.sql`

**Analytical Marts:**

| Mart | Business Question |
|---|---|
| `mart_sla_breach_analysis` | Cities and time slots with highest SLA breach rate |
| `mart_restaurant_prep_delays` | Restaurants causing prep-time-driven late deliveries |
| `mart_refund_drivers` | Refund breakdown by delay, missing items, cancellations |
| `mart_rider_performance` | Riders handling high volumes without increasing lateness |
| `mart_weekly_trends` | Week-over-week order, cancellation, and refund trends |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Java (required for PySpark)
- `pip` package manager

### Installation

```bash
git clone https://github.com/TarjBaxi74/food_del_pipeline.git
cd food_del_pipeline
pip install -e .
```

### Running the Pipeline

**1. Generate raw data:**
```bash
python src/generators/<generator_script>.py
```

**2. Ingest to Bronze (DuckDB):**
```bash
python src/loaders/<bronze_loader>.py
```

**3. Run data profiling:**
```bash
python src/profiling/<profiling_script>.py
```

**4. Run Silver transformations (PySpark):**
```bash
make run_spark
# or directly:
python src/spark_jobs/test_spark.py
```

**5. Load Silver to DuckDB warehouse:**
```bash
python src/loaders/silver_to_duckdb.py
```

**6. Run dbt models:**
```bash
cd dbt_project
dbt run
dbt test
```

---

## 🧪 Testing

Python unit tests are located in `tests/`. dbt tests are in `dbt_project/tests/`, including:
- `assert_refund_pct_sums_to_100.sql`
- Schema tests on all staging models

---

## 📖 Documentation

See [`reference.md`](./reference.md) for the full architecture reference, including detailed layer descriptions, technology rationale, and transformation logic.

---

## 🎯 Expected Outcome

A complete analytics pipeline demonstrating practical modern data engineering — raw simulation, medallion layering, quality profiling, distributed transformation, and analytics modelling — producing actionable insights on delivery performance, operational efficiency, and refund behaviour.
