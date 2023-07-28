import os
import csv
import time
import polars as pl
from numpy import nan

# Get current directory
curr_dir = os.getcwd()

# Load taxi zones from csv file
df_tz = pl.read_csv(f"{curr_dir}/taxi_zone_location_ids.csv", has_header=False)

# Create dictionary with borough names as keys and ids of taxi zones in this borough as values
borough_dict = {}
for borough in df_tz['column_3'].unique().to_list():
    borough_dict[borough] = df_tz.filter(pl.col('column_3') == borough)['column_1'].unique().to_list()

def id_to_borough(puid):
    """
    Return borough name from its id number according to 'borough_dict' dictionary
    """
    for key, value in borough_dict.items():
        if puid in value:
            return key
    return nan

# Prepare csv files for loading into clickhouse table
st = time.time()
columns_green = ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'RatecodeID']
columns_green_new = ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 'dropoff_location_id', 'rate_code_id']

for file in os.listdir('data'):
    if file.split('.')[-1] == 'parquet' and file[:5] == 'green' and f"{file.split('.')[0]}.csv" not in os.listdir('/var/lib/clickhouse/user_files'):
        file_name = file.split('.')[0]
        file_path = f"{curr_dir}/data/{file_name}.parquet"
        print(f"Start for {file_name}...")
        df_green = pl.read_parquet(file_path)
        df_green = df_green.lazy()\
        .with_columns([(pl.lit('green')).alias('car_type'),
                       (pl.col('PULocationID').apply(id_to_borough)).alias('pickup_borough'),
                       (pl.col('DOLocationID').apply(id_to_borough)).alias('dropoff_borough'),
                       (pl.lit(None).alias('airport_fee')),
                       (pl.lit(file_name)).alias('filename')
                      ])\
        .fill_nan(None)\
        .select(['car_type',
                 (pl.col('VendorID').cast(pl.UInt16)),
                 'lpep_pickup_datetime',
                 'lpep_dropoff_datetime',
                 (pl.col('PULocationID').cast(pl.UInt16)),
                 (pl.col('DOLocationID').cast(pl.UInt16)),
                 'pickup_borough',
                 'dropoff_borough',
                 (pl.col('passenger_count').cast(pl.UInt16)),
                 'trip_distance',
                 (pl.col('RatecodeID').cast(pl.UInt16)),
                 'store_and_fwd_flag',
                 (pl.col('payment_type').cast(pl.UInt16)),
                 'fare_amount',
                 'extra',
                 'mta_tax',
                 'tip_amount',
                 'tolls_amount',
                 'improvement_surcharge',
                 'total_amount',
                 'congestion_surcharge',
                 (pl.col('trip_type').cast(pl.UInt16)),
                 'ehail_fee',
                 'airport_fee',
                 'filename'
                ])\
        .rename(dict(zip(columns_green, columns_green_new)))\
        .collect()
#         Write csv files to clickhouse user_files directory
        df_green.write_csv(f"/var/lib/clickhouse/user_files/{file_name}.csv")

fn = time.time()
print(f"Total execuction time: {(fn - st)/60} minutes")