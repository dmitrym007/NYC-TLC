#!/bin/bash

FILES="/var/lib/clickhouse/user_files/yellow_tripdata*.csv"

for file in $FILES
do
  echo "`date`: Inserting from $file..."
  clickhouse-client -q "INSERT INTO nyc_tlc_data.taxi_trips FORMAT CSV" < $file --progress
done;