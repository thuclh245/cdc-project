import random
from faker import Faker
from datetime import datetime
import csv

fake = Faker()

NUM_ROWS = 100_000

STATUSES = [
    "CREATED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED"
]

with open("orders_100k.csv", "w", newline="") as f:

    writer = csv.writer(f)

    writer.writerow([
        "order_id",
        "customer_id",
        "status",
        "amount",
        "created_at",
        "updated_at",
        "deleted"
    ])

    for order_id in range(1, NUM_ROWS + 1):

        created_at = fake.date_time_this_year()

        updated_at = created_at

        writer.writerow([
            order_id,
            random.randint(1, 10000),
            random.choice(STATUSES),
            round(random.uniform(10, 5000), 2),
            created_at,
            updated_at,
            False
        ])

print(f"Generated {NUM_ROWS} rows")