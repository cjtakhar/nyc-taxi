from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, timedelta
from typing import Optional
import psycopg2
import os

app = FastAPI()

# Allow your React dev server(s)
origins = [
    "http://localhost:3000",  # CRA
    "http://localhost:5173",  # Vite
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Database config (matches docker-compose in the container) ----
# In Docker, these are overridden by environment variables:
#   DB_HOST=warehouse-postgres
#   DB_PORT=5432
DB_HOST = os.getenv("DB_HOST", "warehouse-postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "nyc_taxi")
DB_USER = os.getenv("DB_USER", "nyc_taxi")
DB_PASS = os.getenv("DB_PASS", "nyc_taxi")


def get_conn():
    """
    Returns a psycopg2 connection to the warehouse Postgres.
    When running via Docker compose, DB_HOST will be 'warehouse-postgres'.
    For local dev against the exposed port, you can set:
      DB_HOST=localhost, DB_PORT=5433
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def parse_range(start: Optional[date], end: Optional[date]):
    """
    Treats the date range as inclusive on the frontend:
      [start, end]
    and converts it to a [start, end_exclusive) range for SQL.
    """
    if start is None or end is None:
        raise ValueError("start and end must be provided")
    start_ts = start.isoformat()
    end_exclusive = (end + timedelta(days=1)).isoformat()
    return start_ts, end_exclusive


# ---- Simple health check ----
@app.get("/")
def root():
    return {"status": "ok", "message": "NYC Taxi API running"}


# ---- /api/metrics/summary ----
@app.get("/api/metrics/summary")
def summary_metrics(
    start: date = Query(...),
    end: date = Query(...),
):
    """
    Returns:
    {
      totalTrips: number;
      totalRevenue: number;
      avgFare: number;
      avgTipPct: number;  // 0.18 = 18%
    }
    """
    start_ts, end_ts = parse_range(start, end)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          COUNT(*)::bigint AS total_trips,
          COALESCE(SUM(total_amount), 0) AS total_revenue,
          COALESCE(AVG(fare_amount), 0) AS avg_fare,
          COALESCE(
            AVG(
              CASE
                WHEN fare_amount > 0 THEN tip_amount / fare_amount
                ELSE NULL
              END
            ),
            0
          ) AS avg_tip_pct
        FROM raw_nyc_taxi_trips
        WHERE tpep_pickup_datetime >= %s
          AND tpep_pickup_datetime < %s;
        """,
        (start_ts, end_ts),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {
        "totalTrips": int(row[0]),
        "totalRevenue": float(row[1]),
        "avgFare": float(row[2]),
        "avgTipPct": float(row[3]),
    }


# ---- /api/metrics/daily_revenue ----
@app.get("/api/metrics/daily_revenue")
def daily_revenue(
    start: date = Query(...),
    end: date = Query(...),
):
    """
    Returns DailyRevenuePoint[]:
    { trip_date: string; trips: number; total_revenue: number }
    """
    start_ts, end_ts = parse_range(start, end)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          DATE(tpep_pickup_datetime) AS trip_date,
          COUNT(*)::bigint AS trips,
          COALESCE(SUM(total_amount), 0) AS total_revenue
        FROM raw_nyc_taxi_trips
        WHERE tpep_pickup_datetime >= %s
          AND tpep_pickup_datetime < %s
        GROUP BY 1
        ORDER BY 1;
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "trip_date": r[0].isoformat(),
            "trips": int(r[1]),
            "total_revenue": float(r[2]),
        }
        for r in rows
    ]


# ---- /api/metrics/hourly_trips ----
@app.get("/api/metrics/hourly_trips")
def hourly_trips(
    start: date = Query(...),
    end: date = Query(...),
):
    """
    Returns HourlyTripsPoint[]:
    { pickup_hour: number; trips: number; avg_distance: number }
    """
    start_ts, end_ts = parse_range(start, end)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          EXTRACT(HOUR FROM tpep_pickup_datetime)::int AS pickup_hour,
          COUNT(*)::bigint AS trips,
          COALESCE(AVG(trip_distance), 0) AS avg_distance
        FROM raw_nyc_taxi_trips
        WHERE tpep_pickup_datetime >= %s
          AND tpep_pickup_datetime < %s
        GROUP BY 1
        ORDER BY 1;
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "pickup_hour": int(r[0]),
            "trips": int(r[1]),
            "avg_distance": float(r[2]),
        }
        for r in rows
    ]


# ---- /api/metrics/tip_by_payment ----
@app.get("/api/metrics/tip_by_payment")
def tip_by_payment(
    start: date = Query(...),
    end: date = Query(...),
):
    """
    Returns TipByPaymentPoint[]:
    { payment_type: string; trips: number; tip_pct: number }
    """
    start_ts, end_ts = parse_range(start, end)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          CASE payment_type
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided'
            ELSE 'Other'
          END AS payment_label,
          COUNT(*)::bigint AS trips,
          COALESCE(
            AVG(
              CASE
                WHEN fare_amount > 0 THEN tip_amount / fare_amount
                ELSE NULL
              END
            ),
            0
          ) AS avg_tip_pct
        FROM raw_nyc_taxi_trips
        WHERE tpep_pickup_datetime >= %s
          AND tpep_pickup_datetime < %s
        GROUP BY 1
        ORDER BY trips DESC;
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "payment_type": r[0],
            "trips": int(r[1]),
            "tip_pct": float(r[2]),
        }
        for r in rows
    ]


# Optional: let you run `python main.py` directly instead of uvicorn command
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
