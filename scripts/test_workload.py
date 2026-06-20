import random
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

statuses = ["PAID", "SHIPPED", "CANCELLED"]

print("Starting update workload (10,000 updates)...")
for i in range(10000):
    order_id = random.randint(1000, 101000)
    status = random.choice(statuses)

    cur.execute("""
        UPDATE orders
        SET status = %s, updated_at = %s
        WHERE order_id = %s
    """, (status, datetime.now(), order_id))
    
    if (i + 1) % 2000 == 0:
        print(f"Processed {i + 1} updates")

conn.commit()

print("Starting delete workload (1,000 deletes)...")
for i in range(1000):
    order_id = random.randint(1000, 101000)

    cur.execute("""
        UPDATE orders
        SET deleted = TRUE, updated_at = %s
        WHERE order_id = %s
    """, (datetime.now(), order_id))
    
    if (i + 1) % 200 == 0:
        print(f"Processed {i + 1} deletes")

conn.commit()

cur.close()
conn.close()
print("Workload script completed successfully.")
