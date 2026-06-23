import time

from scripts.database.connection import get_conn
from scripts.common.constants import CUSTOMERS_PER_CYCLE, STREAM_INTERVAL_SECONDS
from scripts.common.utils import sleep_until_next_cycle
from scripts.services.customer_service import insert_customer


def run_customer_stream():
    conn = get_conn()

    try:
        print(
            "Customer stream started: "
            f"{CUSTOMERS_PER_CYCLE} new customers every {STREAM_INTERVAL_SECONDS}s."
        )

        while True:
            cycle_started_at = time.monotonic()
            created = 0

            for _ in range(CUSTOMERS_PER_CYCLE):
                try:
                    insert_customer(conn)
                    created += 1
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR CUSTOMER STREAM] insert_customer: {e}")

            print(f"[CUSTOMER CYCLE] created={created}/{CUSTOMERS_PER_CYCLE}")
            sleep_until_next_cycle(cycle_started_at, STREAM_INTERVAL_SECONDS)

    finally:
        conn.close()


if __name__ == "__main__":
    run_customer_stream()
