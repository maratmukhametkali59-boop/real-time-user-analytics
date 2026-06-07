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
    "view",
    "click",
    "add_to_cart",
    "purchase"
]

PAGES = [
    "/",
    "/catalog",
    "/products",
    "/cart",
    "/checkout"
]

while True:
    event_type = random.choice(EVENT_TYPES)

    event = {
        "user_id": random.randint(1, 100),
        "session_id": f"sess_{random.randint(1000, 9999)}",
        "product_id": random.randint(1, 500),
        "price": round(random.uniform(1000, 50000), 2),
        "event_type": event_type,
        "page": random.choice(PAGES),
        "event_time": datetime.now().isoformat()
    }

    producer.send(
        "user_events_v3",
        value=event
    )

    print(f"Sent: {event}")

    time.sleep(1)