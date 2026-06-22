import random
import time
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from faker import Faker
import psycopg2
from db_config import get_postgres_config, get_postgres_target

fake = Faker()

print(f"Connecting to PostgreSQL: {get_postgres_target()}")
conn = psycopg2.connect(**get_postgres_config())

conn.autocommit = True
cur = conn.cursor()

# ===== CONFIG =====
INSERT_RATE = 1      # mỗi vòng insert bao nhiêu row
UPDATE_RATE = 1
DELETE_RATE = 1

sleep_time = 10      # tốc độ stream (giây)

statuses = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]

# ===== INSERT =====
def insert_order():
    order_id = random.randint(100000, 999999)
    customer_id = random.randint(1, 1000)
    status = "CREATED"
    amount = round(random.uniform(10, 500), 2)
    now = datetime.now(timezone.utc)

    cur.execute("""
        INSERT INTO orders (
            order_id, customer_id, status, amount, created_at, updated_at, deleted
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (order_id, customer_id, status, amount, now, now, False))

    print(f"[INSERT] order_id={order_id}", flush=True)


# ===== UPDATE =====
def update_order():
    cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1;")
    row = cur.fetchone()
    if not row:
        return

    order_id = row[0]
    new_status = random.choice(statuses)
    new_amount = round(random.uniform(10, 1000), 2)

    cur.execute("""
        UPDATE orders
        SET status = %s,
            amount = %s,
            updated_at = %s
        WHERE order_id = %s
    """, (new_status, new_amount, datetime.now(timezone.utc), order_id))

    print(f"[UPDATE] order_id={order_id} -> {new_status}", flush=True)


# ===== DELETE (soft delete) =====
def delete_order():
    cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1;")
    row = cur.fetchone()
    if not row:
        return

    order_id = row[0]

    cur.execute("""
        UPDATE orders
        SET deleted = TRUE,
            updated_at = %s
        WHERE order_id = %s
    """, (datetime.now(timezone.utc), order_id))

    print(f"[DELETE] order_id={order_id}", flush=True)


# ===== LOOP STREAM =====
print("🚀 Starting CDC stream generator...", flush=True)

while True:
    try:
        # INSERT batch
        for _ in range(INSERT_RATE):
            insert_order()

        # UPDATE batch
        for _ in range(UPDATE_RATE):
            update_order()

        # DELETE batch
        for _ in range(DELETE_RATE):
            delete_order()

        time.sleep(sleep_time)

    except Exception as e:
        print("ERROR:", e, flush=True)
        time.sleep(2)
