#!/bin/bash

parent_path=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd -P)
cd "$parent_path"

cat weather_observations.csv | clickhouse-client --database=nyc_tlc_data -q "INSERT INTO weather_observations (observation_date, average_wind_speed, precipitation, snowfall, snow_depth, max_temperature, min_temperature) FORMAT CSV"
# cat taxi_zone_location_ids.csv | clickhouse-client --database=nyc_tlc_data -q "INSERT INTO taxi_zones (location_id, zone, borough) FORMAT CSV"