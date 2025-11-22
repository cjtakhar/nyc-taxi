-- models/dim_locations.sql

with all_locations as (

    -- Collect all pickup locations
    select
        pickup_location_id as location_id
    from {{ ref('stg_taxi_trips') }}

    union

    -- Collect all dropoff locations
    select
        dropoff_location_id as location_id
    from {{ ref('stg_taxi_trips') }}

),

deduped as (

    select
        location_id
    from all_locations
    where location_id is not null
    group by location_id

),

-- Optional: placeholder columns so you can join real metadata later
final as (

    select
        location_id,
        cast(null as varchar) as borough,
        cast(null as varchar) as zone_name,
        cast(null as varchar) as service_zone
    from deduped

)

select * from final;
