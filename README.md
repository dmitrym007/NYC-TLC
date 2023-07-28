# NYC Taxi and Limousine Commission

The raw data of [the NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and the summary of weather data in New York that comes from [National Climatic Data Center](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail) are used to download, process and load in Clickhouse database to make some analysis.

### OS and installed packages

Operating system version:
Python 3.10 with package installed: Polars
Clickhouse ([installation](https://clickhouse.com/docs/en/install))

### Download raw data

`./download_nyc_taxi_data.sh`

### Initialize database and set up schema

`./initialize_database_schema.sh`

### Load taxi zones and weather observation data

`./load_weather_and_zones.sh`


### Process parquet files in Python using Polars library and write in csv format in clickhouse user_files directory

Process and write fhv_tripdata files  
`python3 fhv_parquet_to_csv.py`

Process and write fhvhv_tripdata files  
`python3 fhvhv_parquet_to_csv.py`

Process and write green_tripdata files  
`python3 green_parquet_to_csv.py`

Process and write yellow_tripdata files starting from 2011  
`python3 yellow_from_2011_parquet_to_csv.py`

### Load csv files to Clickhouse database

`./import_fhv.sh`  
`./import_fhvhv.sh`  
`./import_green.sh`  
`./import_yellow.sh`

