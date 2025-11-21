with source as (

    select
        VENDORID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATECODEID,
        PULOCATIONID,
        DOLOCATIONID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        TIP_AMOUNT,
        TOTAL_AMOUNT
    from {{ source('raw', 'raw_nyc_taxi_trips') }}

),

renamed as (

    select
        vendorid as vendor_id,
        tpep_pickup_datetime as pickup_ts,
        tpep_dropoff_datetime as dropoff_ts,
        passenger_count,
        trip_distance,
        ratecodeid as rate_code_id,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount
    from source

)

select * from renamed
