🚖 NYC Taxi Data Pipeline — Modern Data Stack (Airflow + Postgres + dbt)

This project implements a full modern data engineering pipeline using:

Apache Airflow – workflow orchestration

Postgres – data warehouse

dbt – transformations & analytics layer

Docker Compose – reproducible local environment

Python – bulk ingestion (Parquet → Postgres)

It ingests real NYC Yellow Taxi data (2023), loads it into a warehouse, transforms it using dbt, and prepares the data for analytics and dashboards.

Architecture

          ┌─────────────┐
          │  Parquet     │
          │ NYC Taxi     │
          └──────┬──────┘
                 │
                 ▼
        ┌──────────────────┐
        │   Airflow DAG    │
        │ bulk_load task   │
        └──────┬───────────┘
               │
               ▼
     ┌──────────────────┐
     │  Warehouse (PG)  │
     │ raw_nyc_taxi...  │
     └──────┬───────────┘
            │
            ▼
      ┌────────────────┐
      │    dbt         │
      │ staging/marts  │
      └────────────────┘


🧩 Components
1. Airflow

Runs inside Docker

Hosts the DAG: nyc_taxi_bulk_load_pipeline

Responsible for orchestrating data ingestion

2. Postgres Warehouse

Two Postgres services are defined:

airflow-postgres → Airflow metadata

warehouse-postgres → NYC taxi warehouse (nyc_taxi database)

3. dbt

Performs transformations (stg_taxi_trips model)

Creates a clean schema ready for analytics

🚀 Features
✨ Raw data ingestion (Parquet → Postgres)

Using a Python script:

airflow/scripts/bulk_load_nyc_taxi.py

The script:

Reads a Parquet file

Subsets and orders columns

Creates a raw table

Performs a high-performance COPY into Postgres

✨ Orchestration with Airflow

DAG located in:

airflow/dags/nyc_taxi_pipeline_dag.py


Runs the ingestion task:

bulk_load_parquet_to_postgres

✨ dbt models

Located under:

dbt/


stg_taxi_trips.sql

schema.yml

dbt_project.yml

🛠️ Getting Started
Prerequisites

Docker Desktop installed

Python (optional, only needed if editing scripts locally)

🔧 Setup Instructions

Clone the repo and go to the root:

git clone <repo>
cd nyc-taxi


Ensure your folder structure looks like:

nyc-taxi/
  airflow/
    dags/
    scripts/
  data/
    yellow_tripdata_2023-01.parquet
  dbt/
  docker-compose.yml

▶️ Start the Pipeline
1. Initialize Airflow
docker compose up airflow-init

2. Start Airflow Webserver + Scheduler
docker compose up -d airflow-webserver airflow-scheduler


Open the UI:

👉 http://localhost:8080

Default login (if you created user manually):

admin / admin

🧪 Run the Pipeline

In the Airflow UI:

Turn on nyc_taxi_bulk_load_pipeline

Click Trigger DAG

Monitor the task in the Graph or Grid view.

📊 Validate the Load

Connect to warehouse Postgres:

psql -h localhost -p 5433 -U nyc_taxi -d nyc_taxi


Password: nyc_taxi

Check the table:

SELECT COUNT(*) FROM raw_nyc_taxi_trips;


If rows appear → the ingestion succeeded!

🧱 dbt Transformations

Inside the Airflow webserver container:

docker exec -it airflow-webserver bash
cd /opt/airflow/dags/dbt/nyc_taxi
dbt run
dbt test


This builds:

staging models

marts (if added)

schema tests

🎯 Roadmap (optional next steps)

Load multiple months of data

Add a mart model (fact table)

Build dashboards in Looker Studio or Metabase

Add alerts (Slack/Email)

Add S3/GCS ingestion

Deploy on Airflow Cloud or MWAA

📌 Summary

This project demonstrates:

✔ Data ingestion engineering
✔ Workflow orchestration (Airflow)
✔ Data modeling (dbt)
✔ Warehouse design (Postgres RAW layer)
✔ End-to-end MDS pipeline building
✔ Docker-based reproducible environments
