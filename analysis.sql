-- How many trips do we have?
SELECT COUNT(*) FROM raw_nyc_taxi_trips;

-- Time range covered
SELECT 
  MIN(tpep_pickup_datetime) AS first_pickup,
  MAX(tpep_pickup_datetime) AS last_pickup
FROM raw_nyc_taxi_trips;

-- Trips per day
SELECT 
  DATE(tpep_pickup_datetime) AS trip_date,
  COUNT(*) AS trips
FROM raw_nyc_taxi_trips
GROUP BY 1
ORDER BY 1;

-- Revenue per day
SELECT 
  DATE(tpep_pickup_datetime) AS trip_date,
  SUM(total_amount) AS total_revenue
FROM raw_nyc_taxi_trips
GROUP BY 1
ORDER BY 1 DESC;

-- Average trip distance by hour of day
SELECT
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
  AVG(trip_distance) AS avg_distance
FROM raw_nyc_taxi_trips
GROUP BY 1
ORDER BY 1;
