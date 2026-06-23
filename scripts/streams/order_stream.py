import random

from scripts.database.connection import get_conn
from scripts.common.utils import sleep_random
from scripts.services.order_service import (
    insert_order,
    update_random_order,
    soft_delete_random_order,
)


def run_order_stream():
    conn = get_conn()

    try:
        print("Order stream started.")

        while True:
            event_type = random.choices(
                population=[
                    "insert_order",
                    "update_order",
                    "soft_delete_order",
                ],
                weights=[
                    75,
                    20,
                    5,
                ],
                k=1,
            )[0]

            try:
                if event_type == "insert_order":
                    insert_order(conn)

                elif event_type == "update_order":
                    update_random_order(conn)

                elif event_type == "soft_delete_order":
                    soft_delete_random_order(conn)

            except Exception as e:
                conn.rollback()
                print(f"[ERROR ORDER STREAM] {event_type}: {e}")

            sleep_random(0.3, 1.5)

    finally:
        conn.close()


if __name__ == "__main__":
    run_order_stream()