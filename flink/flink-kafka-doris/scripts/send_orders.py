import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "47.103.24.91:9092"
TOPIC = "order_sales"

PRODUCTS = [
    ("iPhone 15", 6999.00),
    ("MacBook Pro", 14999.00),
    ("AirPods Pro", 1999.00),
    ("iPad Air", 4999.00),
    ("Apple Watch", 3299.00),
]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

order_id = 1
print(f"开始向 Kafka 发送订单数据，每5秒一条... (topic: {TOPIC})")

while True:
    product = random.choice(PRODUCTS)
    payload = {
        "order_id": order_id,
        "user_id": random.randint(1001, 9999),
        "product_name": product[0],
        "price": product[1],
        "quantity": random.randint(1, 5),
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    producer.send(TOPIC, payload)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 已发送: order_id={order_id}, "
          f"product={product[0]}, price={product[1]}, qty={payload['quantity']}")
    order_id += 1
    time.sleep(5)
