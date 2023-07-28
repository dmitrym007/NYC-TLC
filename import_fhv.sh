#!/bin/bash

FILES="/var/lib/clickhouse/user_files/fhv_tripdata*.csv"

for file in $FILES
do
  echo "`date`: Inserting from $file..."
  clickhouse-client -q "INSERT INTO nyc_tlc_data.fhv_trips FORMAT CSV" < $file
done;