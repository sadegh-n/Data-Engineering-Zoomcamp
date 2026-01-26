import pandas as pd

df_tripdata = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')
df_zones = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')

df_merged = df_tripdata.merge(df_zones, left_on='PULocationID', right_on='LocationID', how='left')
df_merged = df_merged.rename(columns={'Zone': 'pickup_zone'})

df_merged = df_merged.merge(df_zones, left_on='DOLocationID', right_on='LocationID', how='left')
df_merged = df_merged.rename(columns={'Zone': 'dropoff_zone'})

count_short_trips = len(df_merged[df_merged['trip_distance'] <= 1])
print(f"Trips with distance <= 1 mile: {count_short_trips}")

df_merged['pickup_date'] = df_merged['lpep_pickup_datetime'].dt.date

longest_trip_day = (
    df_merged[df_merged['trip_distance'] < 100]
    .groupby('pickup_date')['trip_distance']
    .max()
    .idxmax()
)
print(f"The day with the longest trip distance was: {longest_trip_day}")

target_date = pd.to_datetime('2025-11-18').date()
nov_18_data = df_merged[df_merged['lpep_pickup_datetime'].dt.date == target_date]

top_zone = nov_18_data.groupby('pickup_zone')['total_amount'].sum().idxmax()
print(f"Zone with largest total amount on Nov 18: {top_zone}")

harlem_trips = df_merged[df_merged['pickup_zone'] == "East Harlem North"]

result = harlem_trips.groupby('dropoff_zone')['tip_amount'].max().idxmax()
max_tip = harlem_trips.groupby('dropoff_zone')['tip_amount'].max().max()

print(f"From East Harlem North, the largest tip was ${max_tip} for a trip to: {result}")
