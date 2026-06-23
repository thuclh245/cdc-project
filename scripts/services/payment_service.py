import random
from datetime import datetime
from faker import Faker

from scripts.common.constants import PAYMENT_METHODS
from scripts.common.utils import log_event


fake = Faker("vi_VN")


def insert_payment(conn, order_id, payment_amount):
    payment_status = random.choice(["SUCCESS", "FAILED", "PENDING"])
    paid_at = datetime.now() if payment_status == "SUCCESS" else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments (
                order_id,
                payment_method,
                payment_status,
                payment_amount,
                transaction_code,
                paid_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING payment_id, payment_status
            """,
            (
                order_id,
                random.choice(PAYMENT_METHODS),
                payment_status,
                payment_amount,
                fake.uuid4(),
                paid_at,
            ),
        )

        payment_id, payment_status = cur.fetchone()

    return payment_id, payment_status


def update_order_payment_status(conn, order_id, payment_status):
    if payment_status == "SUCCESS":
        order_payment_status = "PAID"
    elif payment_status == "FAILED":
        order_payment_status = "FAILED"
    else:
        order_payment_status = "UNPAID"

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
            SET payment_status = %s
            WHERE order_id = %s
            """,
            (order_payment_status, order_id),
        )


def update_random_payment(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payment_id, order_id, payment_status
            FROM payments
            WHERE deleted_at IS NULL
              AND payment_status IN ('PENDING', 'FAILED')
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

        if row is None:
            return None

        payment_id, order_id, old_status = row
        new_status = random.choice(["SUCCESS", "FAILED"])
        paid_at = datetime.now() if new_status == "SUCCESS" else None

        cur.execute(
            """
            UPDATE payments
            SET payment_status = %s,
                paid_at = %s
            WHERE payment_id = %s
            """,
            (new_status, paid_at, payment_id),
        )

        if new_status == "SUCCESS":
            cur.execute(
                """
                UPDATE orders
                SET payment_status = 'PAID'
                WHERE order_id = %s
                """,
                (order_id,),
            )

    conn.commit()
    log_event("UPDATE PAYMENT", f"payment_id={payment_id}, {old_status}->{new_status}")
    return payment_id