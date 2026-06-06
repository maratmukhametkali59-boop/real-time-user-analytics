import random
import time
from datetime import datetime

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="analytics",
    user="de_user",
    password="de_pass"
)

cursor = conn.cursor()

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
    user_id = random.randint(1, 100)
    event_type = random.choice(EVENT_TYPES)
    page = random.choice(PAGES)
    event_time = datetime.now()

    cursor.execute(
        """
        INSERT INTO raw_events
        (user_id, event_type, page, event_time)
        VALUES (%s, %s, %s, %s)
        """,
        (user_id, event_type, page, event_time)
    )

    conn.commit()

    print(
        f"Inserted: user={user_id}, "
        f"event={event_type}, "
        f"page={page}"
    )

    time.sleep(1)