import os
import glob
import pandas as pd
import psycopg2
from io import StringIO

# Directory inside the Airflow container that holds all Parquet files
# This maps to ./data on your host via docker-compose
DATA_DIR = "/opt/airflow/data"


def bulk_load_parquet_to_postgres():
    """
    Bulk loads NYC Yellow Taxi data for 2025 into Postgres.

    - Finds all /opt/airflow/data/yellow_tripdata_2025-*.parquet
    - Truncates raw_nyc_taxi_trips (so reruns don't duplicate rows)
    - Appends each month's rows into raw_nyc_taxi_trips using COPY
    """

    # -------------------------
    # 1) Find 2025 parquet files
    # -------------------------
    parquet_files = sorted(
        glob.glob(os.path.join(DATA_DIR, "yellow_tripdata_2025-*.parquet"))
    )

    if not parquet_files:
        raise FileNotFoundError(
            "No 2025 parquet files found in /opt/airflow/data "
            "(expected files like yellow_tripdata_2025-01.parquet)"
        )

    print("📁 Found the following 2025 parquet files:")
    for f in parquet_files:
        print(f"   - {f}")

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
        airport_fee DOUBLE PRECISION,
        cbd_congestion_fee DOUBLE PRECISION
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    # -------------------------
    # 4) Truncate existing data (avoid duplicates on rerun)
    # -------------------------
    print("🧹 Truncating existing data in raw_nyc_taxi_trips...")
    cur.execute("TRUNCATE TABLE raw_nyc_taxi_trips;")
    conn.commit()

    # -------------------------
    # 5) Load each parquet into Postgres
    # -------------------------
    # These must match the *Parquet* column names exactly:
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
        "Airport_fee",
        "cbd_congestion_fee",
    ]

    int_like_cols = [
        "VendorID",
        "passenger_count",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
    ]

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
        airport_fee,
        cbd_congestion_fee
    )
    FROM STDIN WITH (FORMAT CSV, NULL '');
    """

    for parquet_path in parquet_files:
        print(f"📦 Loading {parquet_path} ...")

        # Read parquet
        df = pd.read_parquet(parquet_path)

        # Subset & reorder columns in a consistent way
        df = df[columns]

        # Clean integer-like columns so they don't become 1.0, 2.0, etc.
        for col in int_like_cols:
            if col in df.columns:
                df[col] = df[col].round().astype("Int64")

        # Write to in-memory CSV buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)

        # Bulk copy to Postgres
        cur.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f"✅ Finished loading {parquet_path} (rows: {len(df)})")

    cur.close()
    conn.close()

    print("🎉 All 2025 Parquet files loaded successfully into raw_nyc_taxi_trips!")


# 🔻 ADD THIS AT THE BOTTOM 🔻
if __name__ == "__main__":
    bulk_load_parquet_to_postgres()
