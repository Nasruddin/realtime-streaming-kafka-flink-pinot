from kafka import KafkaConsumer
import json
import os



# CONFIG
usersLimit         = 1000
orderInterval      = 100
mysqlHost          = os.environ.get("MYSQL_SERVER", "mysql")
mysqlPort          = '3306'
mysqlUser          = 'mysqluser'
mysqlPass          = 'mysqlpw'
debeziumHostPort   = 'debezium:8083'
kafkaHostPort      = f"{os.environ.get('KAFKA_BROKER_HOSTNAME', 'kafka')}:{os.environ.get('KAFKA_BROKER_PORT', '29092')}"

print(f"Kafka broker: {kafkaHostPort}")

consumer = KafkaConsumer('rides',  # Topic Name
                        bootstrap_servers=kafkaHostPort,
                        auto_offset_reset='earliest',  # Start reading from the beginning
                        enable_auto_commit=True,
                        group_id='ride-consumer-group',
                        # Converts the Kafka message back into a Python dictionary.
                        key_deserializer=lambda k: json.loads(k.decode("utf-8")) if k else None,  # Handle None keys properly
                        value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # Deserialize value
)

print("Listening for messages...")

# Consume messages
for message in consumer:
    try:
        print(f"Received message: {message}")
        # Extract key and value
        key = message.key  # This is already deserialized
        value = message.value # Raw message (string format)
        
        # Print the message
        print("\nReceived Kafka Message:")
        print(f"Key: {key}")
        print(f"Value: {value}")

    except Exception as e:
        print(f"Error processing message: {e}")
