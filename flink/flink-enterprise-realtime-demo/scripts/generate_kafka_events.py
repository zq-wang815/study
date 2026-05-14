#!/usr/bin/env python3
import json
import time

from kafka import KafkaProducer


BOOTSTRAP = "47.103.24.91:9092"


def producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
    )


def main():
    p = producer()

    base = "2026-05-14 11:00:"
    order_events = [
        {"order_id": 5001, "user_id": 1001, "product_id": 3001, "quantity": 2, "order_amount": 25.00, "order_status": "CREATED", "order_time": base + "01"},
        {"order_id": 5002, "user_id": 1002, "product_id": 3002, "quantity": 1, "order_amount": 199.00, "order_status": "CREATED", "order_time": base + "05"},
        {"order_id": 5003, "user_id": 1003, "product_id": 3003, "quantity": 1, "order_amount": 89.00, "order_status": "CREATED", "order_time": base + "08"},
        {"order_id": 5004, "user_id": 1001, "product_id": 3002, "quantity": 1, "order_amount": 199.00, "order_status": "CREATED", "order_time": base + "12"},
        {"order_id": 5005, "user_id": 1002, "product_id": 3001, "quantity": 3, "order_amount": 37.50, "order_status": "CREATED", "order_time": base + "15"},
    ]

    payment_events = [
        {"payment_id": 7001, "order_id": 5001, "user_id": 1001, "product_id": 3001, "shop_id": 2001, "payment_amount": 25.00, "payment_status": "SUCCESS", "payment_channel": "ALIPAY", "payment_time": base + "18"},
        {"payment_id": 7002, "order_id": 5002, "user_id": 1002, "product_id": 3002, "shop_id": 2002, "payment_amount": 199.00, "payment_status": "FAILED", "payment_channel": "WECHAT", "payment_time": base + "20"},
        {"payment_id": 7003, "order_id": 5003, "user_id": 1003, "product_id": 3003, "shop_id": 2003, "payment_amount": 89.00, "payment_status": "SUCCESS", "payment_channel": "CARD", "payment_time": base + "24"},
        {"payment_id": 7004, "order_id": 5004, "user_id": 1001, "product_id": 3002, "shop_id": 2002, "payment_amount": 199.00, "payment_status": "FAILED", "payment_channel": "ALIPAY", "payment_time": base + "28"},
        {"payment_id": 7005, "order_id": 5005, "user_id": 1002, "product_id": 3001, "shop_id": 2001, "payment_amount": 37.50, "payment_status": "FAILED", "payment_channel": "WECHAT", "payment_time": base + "33"},
        {"payment_id": 7006, "order_id": 5002, "user_id": 1002, "product_id": 3002, "shop_id": 2002, "payment_amount": 199.00, "payment_status": "FAILED", "payment_channel": "WECHAT", "payment_time": base + "38"},
    ]

    page_view_events = [
        {"event_id": 9001, "user_id": 1001, "page_type": "HOME", "product_id": None, "shop_id": None, "stay_seconds": 6, "view_time": base + "02"},
        {"event_id": 9002, "user_id": 1001, "page_type": "PRODUCT", "product_id": 3002, "shop_id": 2002, "stay_seconds": 10, "view_time": base + "03"},
        {"event_id": 9003, "user_id": 1002, "page_type": "PRODUCT", "product_id": 3002, "shop_id": 2002, "stay_seconds": 8, "view_time": base + "04"},
        {"event_id": 9004, "user_id": 1003, "page_type": "PRODUCT", "product_id": 3002, "shop_id": 2002, "stay_seconds": 7, "view_time": base + "06"},
        {"event_id": 9005, "user_id": 1002, "page_type": "PRODUCT", "product_id": 3002, "shop_id": 2002, "stay_seconds": 9, "view_time": base + "07"},
        {"event_id": 9006, "user_id": 1001, "page_type": "PRODUCT", "product_id": 3002, "shop_id": 2002, "stay_seconds": 11, "view_time": base + "09"},
        {"event_id": 9007, "user_id": 1003, "page_type": "PRODUCT", "product_id": 3002, "shop_id": 2002, "stay_seconds": 5, "view_time": base + "10"},
        {"event_id": 9008, "user_id": 1001, "page_type": "SHOP", "product_id": None, "shop_id": 2001, "stay_seconds": 12, "view_time": base + "11"},
    ]

    for event in order_events:
        p.send("rt_order_events", event)
    for event in payment_events:
        p.send("rt_payment_events", event)
    for event in page_view_events:
        p.send("rt_page_view_events", event)

    p.flush()
    time.sleep(1)
    p.close()
    print("sent demo events")


if __name__ == "__main__":
    main()
