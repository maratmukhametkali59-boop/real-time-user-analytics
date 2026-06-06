import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = [
    "click",
    "view",
    "purchase",
    "add_to_cart"
]

PAGES = [
    "/",
    "/catalog",
    "/products",
    "/cart",
    "/checkout"
]

while True:
    event = {
        "user_id": random.randint(1, 100),
        "event_type": random.choice(EVENT_TYPES),
        "page": random.choice(PAGES),
        "event_time": datetime.now().isoformat()
    }

    producer.send(
        "user_events_v2",
        value=event
    )

    print(f"Sent: {event}")

    time.sleep(1)