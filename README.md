# 🚖 NYC Taxi Data Pipeline

### *A Modern Data Engineering Project (Airflow • Postgres • dbt • Docker)*

This project implements a **full modern data engineering pipeline** using:

* **Apache Airflow** for workflow orchestration
* **Postgres** as a local data warehouse
* **dbt** for transformations and analytics modeling
* **Docker Compose** for a fully reproducible environment
* **Python** for bulk ingestion (Parquet → Postgres)

It ingests **real NYC Yellow Taxi data**, loads it into a warehouse, transforms it with dbt, and prepares it for analytics and dashboards — all locally and fully containerized.

---

## 🏗️ Architecture

```
          ┌───────────────┐
          │   Parquet     │
          │ NYC Taxi Data │ 
          └───────┬───────┘
                  │
                  ▼
      ┌──────────────────────┐
      │      Airflow DAG     │
      │  bulk_load task      │
      └─────────┬────────────┘
                │
                ▼
      ┌──────────────────────┐
      │ Postgres Warehouse   │
      │ raw_nyc_taxi_trips   │
      └─────────┬────────────┘
                │
                ▼
      ┌──────────────────────┐
      │         dbt          │
      │  staging / marts     │
      └──────────────────────┘
```

---

## 🧩 Components

### **1. Apache Airflow**

* Runs inside Docker
* Hosts the DAG: `nyc_taxi_bulk_load_pipeline`
* Orchestrates ingestion + transformation

### **2. Postgres Warehouse**

Two Postgres instances:

* `airflow-postgres` → Airflow metadata
* `warehouse-postgres` → `nyc_taxi` warehouse DB

### **3. dbt**

* Cleans, models, and tests data
* Staging model: `stg_taxi_trips.sql`
* Schema tests in `schema.yml`

---

## 🚀 Features

### ✨ **High-performance raw ingestion (Parquet → Postgres)**

`airflow/scripts/bulk_load_nyc_taxi.py`:

* Reads Parquet
* Normalizes columns
* Creates raw table
* Bulk-loads using Postgres `COPY`

### ✨ **Orchestrated with Airflow**

`airflow/dags/nyc_taxi_pipeline_dag.py`:

* Automates ingestion
* Runs as a single DAG task

### ✨ **dbt transformations**

Under `/dbt`:

* Staging layer
* Data tests
* Marts (optional)

---

## 🛠️ Getting Started

### **Prerequisites**

* Docker Desktop
* (Optional) Python 3.10+

---

## 🔧 Setup

Clone and enter the project:

```bash
git clone https://github.com/cjtakhar/nyc-taxi
cd nyc-taxi
```

Ensure this structure exists:

```
nyc-taxi/
  airflow/
    dags/
    scripts/
  data/
    yellow_tripdata_2023-01.parquet
  dbt/
  docker-compose.yml
```

---

## ▶️ Run the Pipeline

### **1. Initialize Airflow**

```bash
docker compose up airflow-init
```

### **2. Start Airflow Webserver + Scheduler**

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

### Open the UI

👉 [http://localhost:8080](http://localhost:8080)
Login (if manually created):

```
admin / admin
```

---

## 🧪 Run the DAG

In the Airflow UI:

1. Enable `nyc_taxi_bulk_load_pipeline`
2. Click **Trigger DAG**
3. Watch task logs & Graph view

---

## 📊 Validate Load in Postgres

Connect:

```bash
psql -h localhost -p 5433 -U nyc_taxi -d nyc_taxi
# password: nyc_taxi
```

Query:

```sql
SELECT COUNT(*) FROM raw_nyc_taxi_trips;
```

If rows appear → 🎉 ingestion succeeded.

---

## 🧱 Run dbt Models

```bash
docker exec -it airflow-webserver bash
cd /opt/airflow/dags/dbt/nyc_taxi
dbt run
dbt test
```

---

## 🎯 Roadmap

* Load multiple months
* Add fact tables / marts
* Build dashboards (Looker Studio / Metabase)
* Add alerts (Slack / Email)
* Add S3 ingestion
* Deploy on MWAA / Astronomer

---

## 📌 Summary

This project demonstrates:

✔ Data ingestion engineering
✔ Workflow orchestration with Airflow
✔ RAW → staging → marts modeling in dbt
✔ Postgres as a local warehouse
✔ Modern Data Stack principles
✔ Fully containerized reproducible environment

---

