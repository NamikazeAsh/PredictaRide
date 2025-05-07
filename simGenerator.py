import pandas as pd
import numpy as np
from faker import Faker
import random
from geopy.distance import geodesic

fake = Faker()
num_records = 10000 
random.seed(42)

def generate_ride_data(n):

    data = []

    for _ in range(n):
        user_id = fake.uuid4()
        ride_id = fake.uuid4()
        pickup_lat = random.uniform(40.5, 40.9) 
        pickup_lon = random.uniform(-74.0, -73.7)
        dropoff_lat = random.uniform(40.5, 40.9)
        dropoff_lon = random.uniform(-74.0, -73.7)
        duration = random.randint(5, 30) 
        status = random.choice(['completed', 'cancelled', 'in-progress'])
        timestamp = fake.date_time_this_year()
        
        demand = random.randint(0, 10)
        
        distance = geodesic((pickup_lat, pickup_lon), (dropoff_lat, dropoff_lon)).km
        
        data.append([timestamp, user_id, ride_id, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, duration, status, demand, distance])
    
    df = pd.DataFrame(data, columns=['timestamp', 'user_id', 'ride_id', 'pickup_lat', 'pickup_lon', 'dropoff_lat', 'dropoff_lon', 'duration', 'status', 'demand', 'distance'])
    return df

ride_data = generate_ride_data(num_records)
print(ride_data.head())

ride_data.to_csv("rides.csv", index=False)

