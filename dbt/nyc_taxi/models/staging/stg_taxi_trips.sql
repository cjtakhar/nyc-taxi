-- models/staging/stg_taxi_trips.sql

with source as (

    select
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee,
        cbd_congestion_fee
    from {{ source('raw', 'raw_nyc_taxi_trips') }}

),

renamed as (

    select
        VendorID as vendor_id,
        tpep_pickup_datetime as pickup_ts,
        tpep_dropoff_datetime as dropoff_ts,
        passenger_count,
        trip_distance,
        RatecodeID as rate_code_id,
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        congestion_surcharge,
        Airport_fee as airport_fee,
        cbd_congestion_fee
    from source

)

select * from renamed;
