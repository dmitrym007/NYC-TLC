import os
import csv
import time
import polars as pl
from numpy import nan

# Get current directory
curr_dir = os.getcwd()

# Load taxi zones from csv file
df_tz = pl.read_csv(f"{curr_dir}/taxi_zone_location_ids.csv", has_header=False)

# Create dictionary with company names as keys and base numbers as values
company_dict = {'juno': ['B02907', 'B02908', 'B02914', 'B03035'], 
                'lyft': ['B02510', 'B02844'], 
                'uber': ['B02395', 'B02404', 'B02512', 'B02598', 'B02617', 'B02682', 'B02764', 'B02765', 'B02835', 
                         'B02836', 'B02864', 'B02865', 'B02866', 'B02867', 'B02869', 'B02870', 'B02871', 'B02872', 
                         'B02875', 'B02876', 'B02877', 'B02878', 'B02879', 'B02880', 'B02882', 'B02883', 'B02884', 
                         'B02887', 'B02888', 'B02889'], 
                'via': ['B02800', 'B03136']
               }

def base_to_comp(base_num):
    """
    Return company name from base_num according to 'company_dict' dictionary
    """
    for key, value in company_dict.items():
        if base_num in value:
            return key
    return 'other'

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
columns_fhvhv = ['PULocationID', 'DOLocationID', 'bcf', 'shared_request_flag', 'shared_match_flag', 'access_a_ride_flag', 'wav_request_flag', 'wav_match_flag']
columns_fhvhv_new = ['pickup_location_id', 'dropoff_location_id', 'black_car_fund', 'shared_request', 'shared_match', 'access_a_ride', 'wav_request', 'wav_match']

for file in os.listdir('data'):
    if file.split('.')[-1] == 'parquet' and file[:6] == 'fhvhv_' and f"{file.split('.')[0]}.csv" not in os.listdir('/var/lib/clickhouse/user_files'):
        file_name = file.split('.')[0]
        file_path = f"{curr_dir}/data/{file_name}.parquet"
        print(f"Start for {file_name}...")
        df_fhvhv = pl.read_parquet(file_path)
        df_fhvhv = df_fhvhv.lazy()\
        .with_columns([(pl.col('dispatching_base_num').apply(base_to_comp)).alias('company'),
                       (pl.col('PULocationID').apply(id_to_borough)).alias('pickup_borough'),
                       (pl.col('DOLocationID').apply(id_to_borough)).alias('dropoff_borough'),
                       (pl.lit(None).alias('legacy_shared_ride')),
                       (pl.lit(file_name)).alias('filename')
                      ])\
        .fill_nan(None)\
        .select(['hvfhs_license_num', 
                 'company', 
                 'dispatching_base_num', 
                 'originating_base_num', 
                 'request_datetime', 
                 'on_scene_datetime', 
                 'pickup_datetime', 
                 'dropoff_datetime', 
                 'PULocationID', 
                 'DOLocationID', 
                 'pickup_borough', 
                 'dropoff_borough', 
                 'trip_miles', 
                 'trip_time', 
                 'base_passenger_fare', 
                 'tolls', 
                 'bcf', 
                 'sales_tax', 
                 'congestion_surcharge', 
                 'airport_fee', 
                 'tips', 
                 'driver_pay', 
                 'shared_request_flag', 
                 'shared_match_flag', 
                 'access_a_ride_flag', 
                 'wav_request_flag', 
                 'wav_match_flag', 
                 (pl.col('legacy_shared_ride').cast(pl.UInt16, strict=False)), 
                 'filename'
                ])\
        .rename(dict(zip(columns_fhvhv, columns_fhvhv_new)))\
        .collect()
        
        Write csv files to clickhouse user_files directory
        df_fhvhv.write_csv(f"/var/lib/clickhouse/user_files/{file_name}.csv")

fn = time.time()
print(f"Total execuction time: {(fn - st)/60} minutes")