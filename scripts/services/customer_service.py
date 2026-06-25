import random
from uuid import uuid4

from faker import Faker
from psycopg2.errors import UniqueViolation

from scripts.common.constants import GENDERS
from scripts.common.utils import log_event


fake = Faker("vi_VN")


def insert_customer(conn, *, commit=True, log=True, max_attempts=5):
    """Insert a customer with a collision-resistant email.

    A savepoint keeps a rare unique-key collision from aborting a surrounding
    transaction. PostgreSQL's UNIQUE constraint remains the final safeguard.
    """
    for attempt in range(1, max_attempts + 1):
        email = f"customer_{uuid4().hex}@example.com"

        with conn.cursor() as cur:
            cur.execute("SAVEPOINT insert_customer_attempt")
            try:
                cur.execute(
                    """
                    INSERT INTO customers (
                        full_name,
                        email,
                        phone,
                        gender,
                        date_of_birth,
                        address,
                        city,
                        country,
                        customer_status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING customer_id
                    """,
                    (
                        fake.name(),
                        email,
                        fake.phone_number(),
                        random.choice(GENDERS),
                        fake.date_of_birth(minimum_age=18, maximum_age=70),
                        fake.address(),
                        fake.city(),
                        "Vietnam",
                        "ACTIVE",
                    ),
                )
                customer_id = cur.fetchone()[0]
                cur.execute("RELEASE SAVEPOINT insert_customer_attempt")
            except UniqueViolation:
                cur.execute("ROLLBACK TO SAVEPOINT insert_customer_attempt")
                cur.execute("RELEASE SAVEPOINT insert_customer_attempt")
                if attempt == max_attempts:
                    raise
                continue

        if commit:
            conn.commit()
        if log:
            log_event("INSERT CUSTOMER", f"customer_id={customer_id}")
        return customer_id

    raise RuntimeError("could not generate a unique customer email")


def get_random_customer_id(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer_id
            FROM customers
            WHERE deleted_at IS NULL
              AND customer_status = 'ACTIVE'
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

    return row[0] if row else None


def update_random_customer(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer_id, customer_status
            FROM customers
            WHERE deleted_at IS NULL
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

        if row is None:
            return None

        customer_id, old_status = row
        new_status = random.choice(["ACTIVE", "INACTIVE", "BLOCKED"])

        cur.execute(
            """
            UPDATE customers
            SET customer_status = %s
            WHERE customer_id = %s
            """,
            (new_status, customer_id),
        )

    conn.commit()
    log_event("UPDATE CUSTOMER", f"customer_id={customer_id}, {old_status}->{new_status}")
    return customer_id
