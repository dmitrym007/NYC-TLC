CREATE TABLE nyc_tlc_data.taxi_zones (
  location_id UInt16,
  borough String,
  zone String NULL,
  service_zone String NULL
)
ENGINE = MergeTree
ORDER BY (location_id);

CREATE TABLE nyc_tlc_data.weather_observations (
  observation_date Date,
  average_wind_speed Nullable(Float32),
  precipitation Nullable(Float32),
  snowfall Nullable(Float32),
  snow_depth Nullable(Float32),
  max_temperature Nullable(UInt8),
  min_temperature Nullable(Int8)
)
ENGINE = MergeTree
ORDER BY (observation_date);

CREATE TABLE nyc_tlc_data.fhv_trips (
  hvfhs_license_num String,
  company String,
  dispatching_base_num Nullable(String),
  originating_base_num Nullable(String),
  request_datetime Nullable(DateTime64),
  on_scene_datetime Nullable(DateTime64),
  pickup_datetime DateTime64,
  dropoff_datetime DateTime64,
  pickup_location_id Nullable(UInt16),
  dropoff_location_id Nullable(UInt16),
  pickup_borough Nullable(String),
  dropoff_borough Nullable(String),
  trip_miles Nullable(Float64),
  trip_time Nullable(UInt32),
  base_passenger_fare Nullable(Float64),
  tolls Nullable(Float64),
  black_car_fund Nullable(Float64),
  sales_tax Nullable(Float64),
  congestion_surcharge Nullable(Float64),
  airport_fee Nullable(Float64),
  tips Nullable(Float64),
  driver_pay Nullable(Float64),
  shared_request Nullable(String),
  shared_match Nullable(String),
  access_a_ride Nullable(String),
  wav_request Nullable(String),
  wav_match Nullable(String),
  legacy_shared_ride Nullable(UInt16),
  filename String
)
ENGINE = MergeTree
ORDER BY (company, pickup_datetime);

CREATE TABLE nyc_tlc_data.taxi_trips (
  car_type String,
  vendor_id Nullable(UInt16),
  pickup_datetime DateTime64,
  dropoff_datetime DateTime64,
  pickup_location_id Nullable(UInt16),
  dropoff_location_id Nullable(UInt16),
  pickup_borough Nullable(String),
  dropoff_borough Nullable(String),
  passenger_count Nullable(UInt16),
  trip_distance Nullable(Float64),
  rate_code_id Nullable(UInt16),
  store_and_fwd_flag Nullable(String),
  payment_type Nullable(UInt16),
  fare_amount Nullable(Float64),
  extra Nullable(Float64),
  mta_tax Nullable(Float64),
  tip_amount Nullable(Float64),
  tolls_amount Nullable(Float64),
  improvement_surcharge Nullable(Float64),
  total_amount Nullable(Float64),
  congestion_surcharge Nullable(Float64),
  airport_fee Nullable(Float64),
  trip_type Nullable(UInt16),
  ehail_fee Nullable(Float64),
  filename String
)
ENGINE = MergeTree
ORDER BY (car_type, pickup_datetime);

CREATE OR REPLACE VIEW nyc_tlc_data.daily_weather_conditions AS
SELECT
	observation_date,
	precipitation * 2.54 AS prec_cm,
	average_wind_speed * 16.09 / 36 AS avg_wind_ms,
	snowfall * 2.54 * 10 AS snowfall_mm,
	snow_depth * 2.54 * 10 AS snow_depth_mm,
	(max_temperature -32) * 5 / 90 AS max_temperature_tenth_celsius,
	(min_temperature -32) * 5 / 90 AS min_temperature_tenth_celsius
FROM nyc_tlc_data.weather_observations;
