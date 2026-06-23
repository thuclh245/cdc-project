import random
import time

from scripts.common.constants import PRODUCT_EVENTS_PER_CYCLE, PRODUCT_INTERVAL_SECONDS
from scripts.database.connection import get_conn
from scripts.common.utils import sleep_until_next_cycle
from scripts.services.product_service import insert_product, update_random_product


def run_product_stream():
    conn = get_conn()

    try:
        print(
            "Product stream started: "
            f"{PRODUCT_EVENTS_PER_CYCLE} catalog event every "
            f"{PRODUCT_INTERVAL_SECONDS}s."
        )

        while True:
            cycle_started_at = time.monotonic()

            for _ in range(PRODUCT_EVENTS_PER_CYCLE):
                event_type = random.choices(
                    population=["insert_product", "update_product"],
                    weights=[15, 85],
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

            sleep_until_next_cycle(cycle_started_at, PRODUCT_INTERVAL_SECONDS)

    finally:
        conn.close()


if __name__ == "__main__":
    run_product_stream()
