import time

from scripts.common.constants import PAYMENT_UPDATES_PER_CYCLE, STREAM_INTERVAL_SECONDS
from scripts.database.connection import get_conn
from scripts.common.utils import sleep_until_next_cycle
from scripts.services.payment_service import update_random_payment


def run_payment_stream():
    conn = get_conn()

    try:
        print(
            "Payment stream started: "
            f"up to {PAYMENT_UPDATES_PER_CYCLE} transaction updates every "
            f"{STREAM_INTERVAL_SECONDS}s."
        )

        while True:
            cycle_started_at = time.monotonic()
            updated = 0

            for _ in range(PAYMENT_UPDATES_PER_CYCLE):
                try:
                    if update_random_payment(conn) is not None:
                        updated += 1
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR PAYMENT STREAM] {e}")

            print(f"[PAYMENT CYCLE] updated={updated}/{PAYMENT_UPDATES_PER_CYCLE}")
            sleep_until_next_cycle(cycle_started_at, STREAM_INTERVAL_SECONDS)

    finally:
        conn.close()


if __name__ == "__main__":
    run_payment_stream()
