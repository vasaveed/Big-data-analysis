import dask.dataframe as dd
from pathlib import Path
import os
file_path = r"C:/jyo/'CUsersabhilDownloadsflights.csv'.zip"
if not os.path.exists(file_path):
    print(f"Error: The file does not exist at the location: {file_path}")
else:
    print(f"File found: {file_path}")
    df = dd.read_csv(file_path)
    df = dd.read_csv(file_path,
        dtype={
            'fare_amount': 'float64',
            'tip_amount': 'float64',
            'total_amount': 'float64',
            'distance': 'float64',
            'air_time': 'float64',
        },
        assume_missing=True
    )
    print("Number of partitions:", df.npartitions)
    print("Columns:", df.columns)
    if 'payment_type' not in df.columns:
        print("Warning: 'payment_type' column does not exist. Available columns are:", df.columns)
    else:
        payment_type_avg = df.groupby('payment_type')['total_amount'].mean().compute()
        print("Average total amount by payment type:\n", payment_type_avg)
    desc = df.describe().compute()
    print("Descriptive statistics:\n", desc)
    df['flight_date'] = dd.to_datetime(df['time_hour']).dt.date
    flights_per_day = df.groupby('flight_date').size().compute()
    print("Number of flights per day:\n", flights_per_day)
    max_flights_day = flights_per_day.idxmax()
    max_flights_count = flights_per_day.max()
    print(f"Day with highest flights: {max_flights_day} ({max_flights_count} flights)")
    clean_df = df[(df['distance'] > 0) & (df['air_time'] > 0)]
    mean_trip_distance = clean_df['distance'].mean().compute()
    mean_air_time = clean_df['air_time'].mean().compute()
    print(f"Mean trip distance: {mean_trip_distance:.2f} miles")
    print(f"Mean air time: {mean_air_time:.2f} minutes")
