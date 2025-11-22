import os
import pandas as pd
import psycopg2
from io import StringIO

# Path inside the Airflow container where your parquet lives
# (mapped from ./data in docker-compose)
LOCAL_PARQUET_PATH = "/opt/airflow/data/yellow_tripdata_2023-01.parquet"


def bulk_load_parquet_to_postgres():
    """
    1. Read local Parquet file into pandas
    2. Create raw table in Postgres if it doesn't exist
    3. Bulk copy the data into Postgres using COPY
    """

    # -------------------------
    # 1) Read parquet
    # -------------------------
    df = pd.read_parquet(LOCAL_PARQUET_PATH)

    # Ensure columns are in a consistent order and match the table schema
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]

    # Subset and reorder columns (in case parquet has extras)
    df = df[columns]

    # ---- NEW: clean up integer-like columns so they aren't written as "1.0" ----
    int_like_cols = [
        "VendorID",
        "passenger_count",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
    ]

    for col in int_like_cols:
        if col in df.columns:
            # round just in case, then convert to pandas nullable Int64
            df[col] = df[col].round().astype("Int64")

    # -------------------------
    # 2) Connect to warehouse Postgres
    # -------------------------
    host = os.getenv("WAREHOUSE_PG_HOST", "warehouse-postgres")
    dbname = os.getenv("WAREHOUSE_PG_DB", "nyc_taxi")
    user = os.getenv("WAREHOUSE_PG_USER", "nyc_taxi")
    password = os.getenv("WAREHOUSE_PG_PASSWORD", "nyc_taxi")

    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
    )
    cur = conn.cursor()

    # -------------------------
    # 3) Create raw table if not exists
    # -------------------------
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_nyc_taxi_trips (
        vendorid INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance DOUBLE PRECISION,
        ratecodeid INTEGER,
        store_and_fwd_flag VARCHAR(10),
        pulocationid INTEGER,
        dolocationid INTEGER,
        payment_type INTEGER,
        fare_amount DOUBLE PRECISION,
        extra DOUBLE PRECISION,
        mta_tax DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        improvement_surcharge DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        congestion_surcharge DOUBLE PRECISION,
        airport_fee DOUBLE PRECISION
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    # -------------------------
    # 4) Bulk copy using COPY FROM STDIN
    # -------------------------
    # Convert DataFrame to CSV in memory (no header)
    buffer = StringIO()
    # na_rep='' ensures NULLs are empty strings in CSV
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    copy_sql = """
    COPY raw_nyc_taxi_trips (
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
    )
    FROM STDIN WITH (FORMAT CSV, NULL '');
    """

    cur.copy_expert(copy_sql, buffer)
    conn.commit()

    cur.close()
    conn.close()
