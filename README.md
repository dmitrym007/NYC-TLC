# Поездки такси в Нью Йорке

Данные для загрузки в Clickhouse и проведения некоторого анализа взяты с сайтов:
- [данные по поездкам такси](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page);
- [данные о погоде в Нью Йорке](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail).

### Для загрузки и проведения анализа использовались

ОС: Ubuntu 22.04  
Python 3.10 with Polars 0.18.7 library installed  
Clickhouse 23.7 ([installation](https://clickhouse.com/docs/en/install))
Power BI Desktop Version: 2.119.666.0

### Загрузка исходных данных

`./download_nyc_taxi_data.sh`

### Инициализация базы данных и схемы

`./initialize_database_schema.sh`

### Загрузка данных о районах в Нью Йорке и данных о погоде

`./load_weather_and_zones.sh`


### Обработка файлов в форамате parquet и запись в csv формат для загрузки в clickhouse

Process and write fhv_tripdata files  
`python3 fhv_parquet_to_csv.py`

Process and write fhvhv_tripdata files  
`python3 fhvhv_parquet_to_csv.py`

Process and write green_tripdata files  
`python3 green_parquet_to_csv.py`

Process and write yellow_tripdata files starting from 2011  
`python3 yellow_from_2011_parquet_to_csv.py`

### Загрузка csv файлов в базу данных clickhouse

`./import_fhv.sh`  
`./import_fhvhv.sh`  
`./import_green.sh`  
`./import_yellow.sh`

