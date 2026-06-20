import random
from datetime import datetime, UTC
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="cdc_demo",
    user="postgres",
    password="postgres"
)

conn.autocommit = True
cur = conn.cursor()

bad_status = [
    "XXX",
    "UNKNOWN",
    "ERROR",
    "INVALID"
]

while True:

    dirty_type = random.choice([
        "null_customer",
        "negative_amount",
        "bad_status",
        "future_date"
    ])

    order_id = random.randint(8000000, 9000000)

    try:

        if dirty_type == "null_customer":

            cur.execute("""
            INSERT INTO orders
            VALUES (
                %s,
                NULL,
                'CREATED',
                100,
                NOW(),
                NOW(),
                FALSE
            )
            """, (order_id,))

            print("NULL CUSTOMER")

        elif dirty_type == "negative_amount":

            cur.execute("""
            INSERT INTO orders
            VALUES (
                %s,
                100,
                'PAID',
                -500,
                NOW(),
                NOW(),
                FALSE
            )
            """, (order_id,))

            print("NEGATIVE AMOUNT")

        elif dirty_type == "bad_status":

            cur.execute("""
            INSERT INTO orders
            VALUES (
                %s,
                100,
                %s,
                200,
                NOW(),
                NOW(),
                FALSE
            )
            """, (
                order_id,
                random.choice(bad_status)
            ))

            print("BAD STATUS")

        elif dirty_type == "future_date":

            cur.execute("""
            INSERT INTO orders
            VALUES (
                %s,
                100,
                'CREATED',
                200,
                NOW() + interval '365 day',
                NOW(),
                FALSE
            )
            """, (order_id,))

            print("FUTURE DATE")

    except Exception as e:
        print(e)

    import time
    time.sleep(10)
