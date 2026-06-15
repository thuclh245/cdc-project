import random
import time
from datetime import datetime
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="cdc_demo",
    user="postgres",
    password="postgres"
)

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
        INSERT INTO orders(order_id, customer_id, status, amount, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        i,
        customer_id,
        status,
        amount,
        datetime.now(),
        datetime.now()
    ))

    if i % 1000 == 0:
        conn.commit()
        print(f"Inserted {i - start_id + 1} rows")

conn.commit()
cur.close()
conn.close()
print("Data insertion completed successfully.")
