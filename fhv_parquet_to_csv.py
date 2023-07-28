import os
import csv
import polars as pl
from numpy import nan
import time

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

def comp_to_license(name):
    """
    Return hvfhs_license from a company name
    """
    if name == 'juno': return 'HV0002'
    if name == 'lyft': return 'HV0005'
    if name == 'uber': return 'HV0003'
    if name == 'via': return 'HV0004'
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
columns_fhv = ['Affiliated_base_number', 'dropOff_datetime', 'PUlocationID', 'DOlocationID', 'SR_Flag']
columns_fhv_new = ['originating_base_num', 'dropoff_datetime', 'pickup_location_id', 'dropoff_location_id', 'legacy_shared_ride']

for file in os.listdir('data'):
    if file.split('.')[-1] == 'parquet' and file[:4] == 'fhv_' and f"{file.split('.')[0]}.csv" not in os.listdir('/var/lib/clickhouse/user_files'):
        file_name = file.split('.')[0]
        file_path = f"{curr_dir}/data/{file_name}.parquet"
        print(f"Start for {file_name}...")
        df = pl.read_parquet(file_path)
        df = df.lazy()\
        .with_columns([(pl.col('dispatching_base_num').apply(base_to_comp)).alias('company'),
                       (pl.lit(None).alias('request_datetime')),
                       (pl.lit(None).alias('on_scene_datetime')),
                       (pl.col('PUlocationID').apply(id_to_borough)).alias('pickup_borough'),
                       (pl.col('DOlocationID').apply(id_to_borough)).alias('dropoff_borough'),
                       (pl.lit(None).alias('trip_miles')),
                       (pl.lit(None).alias('trip_time')),
                       (pl.lit(None).alias('base_passenger_fare')),
                       (pl.lit(None).alias('tolls')),
                       (pl.lit(None).alias('black_car_fund')),
                       (pl.lit(None).alias('sales_tax')),
                       (pl.lit(None).alias('congestion_surcharge')),
                       (pl.lit(None).alias('airport_fee')),
                       (pl.lit(None).alias('tips')),
                       (pl.lit(None).alias('driver_pay')),
                       (pl.lit(None).alias('shared_request')),
                       (pl.lit(None).alias('shared_match')),
                       (pl.lit(None).alias('access_a_ride')),
                       (pl.lit(None).alias('wav_request')),
                       (pl.lit(None).alias('wav_match')),
                       (pl.lit(file_name)).alias('filename')])\
        .with_columns(pl.col('company').apply(comp_to_license).alias('hvfhs_license_num'))\
        .fill_nan(None)\
        .select(['hvfhs_license_num', 
                 'company', 
                 'dispatching_base_num', 
                 'Affiliated_base_number', 
                 (pl.col('request_datetime').cast(pl.Datetime)), 
                 (pl.col('on_scene_datetime').cast(pl.Datetime)), 
                 'pickup_datetime', 
                 'dropOff_datetime', 
                 (pl.col('PUlocationID').cast(pl.UInt16)), 
                 (pl.col('DOlocationID').cast(pl.UInt16)), 
                 'pickup_borough', 
                 (pl.col('dropoff_borough').cast(pl.Utf8)), 
                 'trip_miles', 
                 (pl.col('trip_time').cast(pl.UInt32)), 
                 'base_passenger_fare', 
                 'tolls', 
                 'black_car_fund', 
                 'sales_tax', 
                 'congestion_surcharge', 
                 'airport_fee', 
                 'tips', 
                 'driver_pay', 
                 (pl.col('shared_request').cast(pl.Utf8)), 
                 (pl.col('shared_match').cast(pl.Utf8)), 
                 (pl.col('access_a_ride').cast(pl.Utf8)), 
                 (pl.col('wav_request').cast(pl.Utf8)), 
                 (pl.col('wav_match').cast(pl.Utf8)), 
                 (pl.col('SR_Flag').cast(pl.UInt16)), 
                 'filename'
                ])\
        .rename(dict(zip(columns_fhv, columns_fhv_new)))\
        .collect()
        
        # Write csv files to clickhouse user_files directory
        df.write_csv(f"/var/lib/clickhouse/user_files/{file_name}.csv")

fnsh = time.time()
print(f"Total execuction time: {(fnsh - st)/60} minutes")