import sys
sys.path.append("/opt/airflow")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.bulk_load_nyc_taxi import bulk_load_parquet_to_postgres

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_bulk_load_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    bulk_load = PythonOperator(
        task_id="bulk_load_parquet_to_postgres",
        python_callable=bulk_load_parquet_to_postgres,
    )
