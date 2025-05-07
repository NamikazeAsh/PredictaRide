import pandas as pd
from kafka import KafkaProducer
import json
import time

# Load the data
ride_data = pd.read_csv("rides.csv")

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Stream each row as a Kafka message
for _, row in ride_data.iterrows():
    message = row.to_dict()
    producer.send('ride_data', value=message)
    # print(f"Sent: {message}")
    print("Sent")
    time.sleep(1)
