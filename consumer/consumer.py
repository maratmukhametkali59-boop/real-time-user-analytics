import json

import psycopg2
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "user_events_v3",
    bootstrap_servers = "localhost:9092",
    auto_offset_reset = "earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="analytics",
    user="de_user",
    password="de_pass"
)

cursor = conn.cursor()

print("Consumer started...")


for message in consumer:
    event = message.value

    cursor.execute(
        """
      INSERT INTO raw_events
(
    user_id,
    session_id,
    product_id,
    price,
    event_type,
    page,
    event_time
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event["user_id"],
            event["session_id"],
            event["product_id"],
            event["price"],
            event["event_type"],
            event["page"],
            event["event_time"]
        )
    )

    conn.commit()

    print(f"Saved event: {event}")