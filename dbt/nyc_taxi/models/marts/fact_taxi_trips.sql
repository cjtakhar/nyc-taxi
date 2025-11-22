select
    pickup_ts,
    dropoff_ts,
    date_trunc('day', pickup_ts) as trip_date,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    rate_code_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee
from {{ ref('stg_taxi_trips') }}
