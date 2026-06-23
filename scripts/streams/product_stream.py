import random

from scripts.database.connection import get_conn
from scripts.common.utils import sleep_random
from scripts.services.product_service import insert_product, update_random_product


def run_product_stream():
    conn = get_conn()

    try:
        print("Product stream started.")

        while True:
            event_type = random.choices(
                population=["insert_product", "update_product"],
                weights=[20, 80],
                k=1,
            )[0]

            try:
                if event_type == "insert_product":
                    insert_product(conn)
                else:
                    update_random_product(conn)

            except Exception as e:
                conn.rollback()
                print(f"[ERROR PRODUCT STREAM] {event_type}: {e}")

            sleep_random(1.0, 3.0)

    finally:
        conn.close()


if __name__ == "__main__":
    run_product_stream()