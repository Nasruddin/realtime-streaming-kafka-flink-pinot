import random, time
from mysql.connector import connect, Error
import json
from kafka import KafkaProducer
import datetime
import uuid
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

producer = KafkaProducer(bootstrap_servers=kafkaHostPort, api_version=(7, 1, 0),
                        key_serializer=lambda k: json.dumps(k).encode("utf-8") if k else None,  # Ensure key is JSON-encoded
                        value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize value
)
events_processed = 0
try:
    with connect(
        host=mysqlHost,
        user=mysqlUser,
        password=mysqlPass,
    ) as connection:
        with connection.cursor() as cursor:
            print("Getting rides for the ride topic")
            cursor.execute("SELECT ride_id,rider_id,driver_id,location_id,amount,ride_status FROM cabservice.rides")
            rides = [{
                "ride_id": str(row[0]),
                "rider_id": row[1],
                "driver_id": row[2],
                "location_id": row[3],
                "amount": row[4],
                "ride_status": row[5]
                }
                for row in cursor
            ]

            for ride in rides:
                key = {
                    "ride_id": ride["ride_id"]
                }
                print(f"Sending ride {ride} to rides topic")
                producer.send('rides', key=key, value=ride)
            producer.flush()

            print("Getting ride ID and Amount as tuples...")
            cursor.execute("SELECT ride_id, amount FROM cabservice.rides")
            ride_prices = [(row[0], row[1]) for row in cursor]
            print(ride_prices)

    connection.close()

except Error as e:
    print(e)

def create_new_ride():
    ride = random.choice(ride_prices)
    return {
        "ride_id": str(uuid.uuid4()),
        "rider_id": str(uuid.uuid4()),
        "driver_id": str(uuid.uuid4()),
        "location_id": 300654,
        "amount": ride[1],
        "ride_status": "In Progress"
    }

while True:
    ride = create_new_ride()
    print(f"Creating & Sending ride {ride} to rides topic")
    key = {
        "ride_id": ride["ride_id"]
    }
    producer.send('rides', key=key, value=ride)

    events_processed += 1
    if events_processed % 100 == 0:
        print(f"{str(datetime.datetime.now())} Flushing after {events_processed} events")
        producer.flush()

    time.sleep(random.randint(orderInterval/5, orderInterval)/1000)

