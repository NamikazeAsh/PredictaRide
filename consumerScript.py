from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ride_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # start from the beginning of the topic
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for messages")

for message in consumer:
    print(message.value)
