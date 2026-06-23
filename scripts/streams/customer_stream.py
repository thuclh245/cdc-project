import random

from scripts.database.connection import get_conn
from scripts.common.utils import sleep_random
from scripts.services.customer_service import insert_customer, update_random_customer


def run_customer_stream():
    conn = get_conn()

    try:
        print("Customer stream started.")

        while True:
            event_type = random.choices(
                population=["insert_customer", "update_customer"],
                weights=[80, 20],
                k=1,
            )[0]

            try:
                if event_type == "insert_customer":
                    insert_customer(conn)
                else:
                    update_random_customer(conn)

            except Exception as e:
                conn.rollback()
                print(f"[ERROR CUSTOMER STREAM] {event_type}: {e}")

            sleep_random(2.0, 5.0)

    finally:
        conn.close()


if __name__ == "__main__":
    run_customer_stream()