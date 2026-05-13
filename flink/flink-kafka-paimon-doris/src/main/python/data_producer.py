"""
Generate random order events and send to Kafka.
Usage: python3 data_producer.py [bootstrap_servers] [topic]
"""

import json
import random
import sys
import time
from datetime import datetime

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

BOOTSTRAP_SERVERS = sys.argv[1] if len(sys.argv) > 1 else "bigdata:9092"
TOPIC = sys.argv[2] if len(sys.argv) > 2 else "demo_orders"
PARTITIONS = 3

PRODUCTS = [
    "iPhone 15", "MacBook Pro", "iPad Air", "AirPods Pro",
    "Apple Watch", "Mac Mini", "Magic Keyboard", "Studio Display",
]


def create_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    existing = admin.list_topics()
    if TOPIC not in existing:
        try:
            admin.create_topics([NewTopic(TOPIC, PARTITIONS, 1)])
            print(f"Created topic: {TOPIC}")
        except TopicAlreadyExistsError:
            pass
    else:
        print(f"Topic already exists: {TOPIC}")
    admin.close()


def main():
    create_topic()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
    )

    order_id = 1
    print(f"Producing to topic: {TOPIC}  ({BOOTSTRAP_SERVERS})")
    try:
        while True:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload = {
                "order_id": order_id,
                "user_id": random.randint(1000, 9999),
                "product_name": random.choice(PRODUCTS),
                "price": round(999 + random.random() * 15000, 2),
                "quantity": random.randint(1, 3),
                "create_time": now,
                "update_time": now,
            }
            future = producer.send(TOPIC, value=payload)
            meta = future.get(timeout=10)
            print(f"Sent: offset={meta.offset} partition={meta.partition} | {payload}")
            order_id += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
