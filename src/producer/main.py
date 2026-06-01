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

# CHANGED: Now pointing to localhost to bridge from the EC2 host into the Docker network
sr_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
avro_serializer = AvroSerializer(sr_client, schema_str)
# CHANGED: Now pointing to localhost on the external mapped port (9092)
producer = Producer({'bootstrap.servers': 'kafka:29092'})
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced record to {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

# 2. Producer Loop
print("Generating a batch of 100 FinStream transactions...")
try:
    # finite 100-record batch
    for _ in range(100):
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
        
        # Reduced sleep time so the batch completes in ~10 seconds
        time.sleep(0.1) 
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Batch complete. Shutting down producer.")