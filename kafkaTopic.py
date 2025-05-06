from confluent_kafka.admin import AdminClient, NewTopic

# Configure Kafka connection
conf = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(conf)

# Define the topic
topic_name = "ride_data"
num_partitions = 1
replication_factor = 1

# Create topic object
new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

# Send create request
fs = admin_client.create_topics([new_topic])

# Wait for creation result
for topic, f in fs.items():
    try:
        f.result()  # Raises exception if creation failed
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
