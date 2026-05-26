import time
import json
import random
from uuid import uuid4
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# 1. Setup
fake = Faker()
schema_str = open("src/producer/schemas/transaction_value.avsc", "r").read()

sr_client = SchemaRegistryClient({'url': 'http://127.0.0.1:8081'})
avro_serializer = AvroSerializer(sr_client, schema_str)

producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced record to {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

# 2. Producer Loop
print("Starting FinStream Producer. Press Ctrl+C to stop.")
try:
    while True:
        transaction = {
            "transaction_id": str(uuid4()),
            "user_id": fake.user_name(),
            "amount": round(random.uniform(1.0, 1000.0), 2),
            "currency": "USD",
            "merchant_id": fake.company(),
            "merchant_category": random.choice(["retail", "online", "food", "travel"]),
            "latitude": float(fake.latitude()),
            "longitude": float(fake.longitude()),
            "timestamp": int(time.time() * 1000)
        }

        # 3. Serialization & Send
        producer.produce(
            topic='transactions-raw',
            value=avro_serializer(transaction, SerializationContext('transactions-raw', MessageField.VALUE)),
            on_delivery=delivery_report
        )
        producer.poll(0)
        time.sleep(1)  # Simulate real-time flow
except KeyboardInterrupt:
    pass
finally:
    producer.flush()