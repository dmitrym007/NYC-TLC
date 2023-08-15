CREATE TABLE nyc_tlc_data.trips_data
(
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `trip_distance` Nullable(Float64),
    `driver_pay` Nullable(Float64)
)
ENGINE = Null;

CREATE TABLE nyc_tlc_data.daily_duration_agg_data
(
    `trip_day` Date,
    `trip_duration` UInt16,
    `trips_count` AggregateFunction(count, UInt64),
    `trip_kms_avg` AggregateFunction(avg, Nullable(Float64)),
    `driver_pay_avg` AggregateFunction(avg, Nullable(Float64))
)
ENGINE = AggregatingMergeTree
ORDER BY (trip_day, trip_duration);

CREATE MATERIALIZED VIEW nyc_tlc_data.daily_duration_agg_data_mv
TO nyc_tlc_data.daily_duration_agg_data
AS
SELECT 
    toDate(pickup_datetime) AS trip_day,
	multiIf(toDateTime(dropoff_datetime) - toDateTime(pickup_datetime) <= 900, 1, 
			toDateTime(dropoff_datetime) - toDateTime(pickup_datetime) > 900 AND toDateTime(dropoff_datetime) - toDateTime(pickup_datetime) <= 2400, 2, 
			toDateTime(dropoff_datetime) - toDateTime(pickup_datetime) > 2400, 3, 4) AS trip_duration,
    countState() AS trips_count,
    avgState(trip_distance * 1.609) AS trip_kms_avg,
    avgState(driver_pay) AS driver_pay_avg
FROM nyc_tlc_data.trips_data
GROUP BY trip_day, trip_duration;

CREATE OR REPLACE VIEW nyc_tlc_data.daily_duration_agg_data_v
AS 
SELECT 
	trip_day,
	trip_duration,
	countMerge(trips_count) AS trips_count,
	avgMerge(trip_kms_avg) AS trip_kms_avg,
	avgMerge(driver_pay_avg) AS driver_pay_avg
FROM nyc_tlc_data.daily_duration_agg_data_mv
WHERE toYear(trip_day) > 2010 AND toYear(trip_day) < 2023  
GROUP BY trip_day, trip_duration;

CREATE TABLE nyc_tlc_data.hourly_agg_data
(
    `trip_hour` DateTime,
    `trips_count` AggregateFunction(count, UInt64),
    `trip_kms_avg` AggregateFunction(avg, Nullable(Float64)),
    `driver_pay_avg` AggregateFunction(avg, Nullable(Float64))
)
ENGINE = AggregatingMergeTree
ORDER BY (trip_hour);

CREATE MATERIALIZED VIEW nyc_tlc_data.hourly_agg_data_mv
TO nyc_tlc_data.hourly_agg_data
AS
SELECT
    toDateTime(toStartOfHour(pickup_datetime)) AS trip_hour,
    countState() AS trips_count,
    avgState(trip_distance * 1.609) AS trip_kms_avg,
    avgState(driver_pay) AS driver_pay_avg
FROM nyc_tlc_data.trips_data
GROUP BY trip_hour;

CREATE OR REPLACE VIEW nyc_tlc_data.hourly_agg_data_v
AS 
SELECT 
	trip_hour,
	countMerge(trips_count) AS trips_count,
	avgMerge(trip_kms_avg) AS trip_kms_avg,
	avgMerge(driver_pay_avg) AS driver_pay_avg
FROM nyc_tlc_data.hourly_agg_data_mv
WHERE toYear(trip_hour) > 2010 AND toYear(trip_hour) < 2023  
GROUP BY trip_hour;

CREATE TABLE nyc_tlc_data.daily_agg_data
(
    `trip_day` Date,
    `trips_count` UInt32,
    `trip_kms_avg` Nullable(Float64),
    `driver_pay_avg` Nullable(Float64)
)
ENGINE = SummingMergeTree()
ORDER BY (trip_day);

CREATE MATERIALIZED VIEW nyc_tlc_data.daily_agg_data_mv
TO nyc_tlc_data.daily_agg_data
AS
SELECT
    toDate(toStartOfDay(trip_hour)) AS trip_day,
    countMerge(trips_count) AS trips_count,
    avgMerge(trip_kms_avg) AS trip_kms_avg,
    avgMerge(driver_pay_avg) AS driver_pay_avg
FROM nyc_tlc_data.hourly_agg_data
GROUP BY trip_day;

CREATE TABLE nyc_tlc_data.trips_data_rsbl
(
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `trip_distance` Nullable(Float64),
    `fare_amount` Nullable(Float64)
)
ENGINE = Null;

CREATE TABLE nyc_tlc_data.daily_agg_rsbl_data
(
    `trip_day` Date,
    `trips_count` AggregateFunction(count, UInt64),
    `trip_kms_avg` AggregateFunction(avg, Nullable(Float64)),
    `fare_amount_avg` AggregateFunction(avg, Nullable(Float64))
)
ENGINE = AggregatingMergeTree
ORDER BY trip_day;

CREATE MATERIALIZED VIEW nyc_tlc_data.daily_agg_rsbl_data_mv
TO nyc_tlc_data.daily_agg_rsbl_data
AS
SELECT
    toDate(pickup_datetime) AS trip_day,
    countState() AS trips_count,
    avgState(trip_distance * 1.609) AS trip_kms_avg,
    avgState(fare_amount) AS fare_amount_avg
FROM nyc_tlc_data.trips_data_rsbl
GROUP BY trip_day;

CREATE OR REPLACE VIEW nyc_tlc_data.daily_agg_rsbl_data_v
AS 
SELECT 
	trip_day,
	countMerge(trips_count) AS trips_count,
	avgMerge(trip_kms_avg) AS trip_kms_avg,
	avgMerge(fare_amount_avg) AS fare_amount_avg
FROM nyc_tlc_data.daily_agg_rsbl_data_mv
WHERE toYear(trip_day) > 2010 AND toYear(trip_day) < 2023  
GROUP BY trip_day;


INSERT INTO nyc_tlc_data.trips_data
	(pickup_datetime, dropoff_datetime, trip_distance, driver_pay)
SELECT 
	pickup_datetime,
	dropoff_datetime,
	trip_miles,
	driver_pay
FROM nyc_tlc_data.fhv_trips
WHERE dropoff_datetime > pickup_datetime;

INSERT INTO nyc_tlc_data.trips_data
	(pickup_datetime, dropoff_datetime, trip_distance, driver_pay)
SELECT 
	pickup_datetime,
	dropoff_datetime,
	trip_distance,
	total_amount
FROM nyc_tlc_data.taxi_trips
WHERE dropoff_datetime > pickup_datetime;

INSERT INTO nyc_tlc_data.trips_data_rsbl
	(pickup_datetime, dropoff_datetime, trip_distance, fare_amount)
SELECT 
	pickup_datetime,
	dropoff_datetime,
	trip_miles,
	base_passenger_fare
FROM nyc_tlc_data.fhv_trips
WHERE isNull(base_passenger_fare) OR base_passenger_fare > 1.5;

INSERT INTO nyc_tlc_data.trips_data_rsbl
	(pickup_datetime, dropoff_datetime, trip_distance, fare_amount)
SELECT 
	pickup_datetime,
	dropoff_datetime,
	trip_distance,
	fare_amount
FROM nyc_tlc_data.taxi_trips
WHERE isNull(fare_amount) OR fare_amount > 1.5;
