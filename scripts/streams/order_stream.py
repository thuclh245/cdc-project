import random
import time

from scripts.common.constants import ORDERS_PER_CYCLE, STREAM_INTERVAL_SECONDS
from scripts.database.connection import get_conn
from scripts.common.utils import sleep_until_next_cycle
from scripts.services.order_service import (
    insert_order,
    update_random_order,
    soft_delete_random_order,
)


def run_order_stream():
    conn = get_conn()

    try:
        print(
            "Order stream started: "
            f"about {ORDERS_PER_CYCLE} orders every {STREAM_INTERVAL_SECONDS}s."
        )

        while True:
            cycle_started_at = time.monotonic()
            created = 0

            for _ in range(ORDERS_PER_CYCLE):
                try:
                    # A realtime order always gets one payment transaction.
                    insert_order(conn, payment_probability=1.0)
                    created += 1
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR ORDER STREAM] insert_order: {e}")

            # Existing orders change state less frequently than new orders arrive.
            if random.random() < 0.35:
                try:
                    update_random_order(conn)
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR ORDER STREAM] update_order: {e}")

            if random.random() < 0.03:
                try:
                    soft_delete_random_order(conn)
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR ORDER STREAM] soft_delete_order: {e}")

            print(f"[ORDER CYCLE] created={created}/{ORDERS_PER_CYCLE}")
            sleep_until_next_cycle(cycle_started_at, STREAM_INTERVAL_SECONDS)

    finally:
        conn.close()


if __name__ == "__main__":
    run_order_stream()
