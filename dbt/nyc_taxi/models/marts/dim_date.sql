with source as (

    select *
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
