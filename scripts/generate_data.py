import random
import time
from datetime import datetime
import psycopg2
from db_config import get_postgres_config, get_postgres_target

print(f"Connecting to PostgreSQL: {get_postgres_target()}")
conn = psycopg2.connect(**get_postgres_config())

cur = conn.cursor()

statuses = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]

start_id = 1000
num_rows = 100000

print(f"Starting insertion of {num_rows} rows...")
for i in range(start_id, start_id + num_rows):
    customer_id = random.randint(1, 10000)
    status = random.choice(statuses)
    amount = round(random.uniform(10000, 5000000), 2)

    cur.execute("""
        INSERT INTO orders(order_id, customer_id, status, amount, created_at, updated_at, deleted)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        i,
        customer_id,
        status,
        amount,
        datetime.now(),
        datetime.now(),
        False
    ))

    if i % 1000 == 0:
        conn.commit()
        print(f"Inserted {i - start_id + 1} rows")

conn.commit()
cur.close()
conn.close()
print("Data insertion completed successfully.")
