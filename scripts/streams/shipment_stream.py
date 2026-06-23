import time

from scripts.common.constants import (
    SHIPMENTS_CREATED_PER_CYCLE,
    SHIPMENT_UPDATES_PER_CYCLE,
    STREAM_INTERVAL_SECONDS,
)
from scripts.common.utils import sleep_until_next_cycle
from scripts.database.connection import get_conn
from scripts.services.shipment_service import (
    advance_random_shipment,
    insert_shipment_for_paid_order,
)


def run_shipment_stream():
    conn = get_conn()

    try:
        print(
            "Shipment stream started: "
            f"up to {SHIPMENTS_CREATED_PER_CYCLE} creates and "
            f"{SHIPMENT_UPDATES_PER_CYCLE} updates every "
            f"{STREAM_INTERVAL_SECONDS}s."
        )

        while True:
            cycle_started_at = time.monotonic()
            created = 0
            updated = 0

            for _ in range(SHIPMENTS_CREATED_PER_CYCLE):
                try:
                    if insert_shipment_for_paid_order(conn) is not None:
                        created += 1
                except Exception as exc:
                    conn.rollback()
                    print(f"[ERROR SHIPMENT STREAM] insert: {exc}")

            for _ in range(SHIPMENT_UPDATES_PER_CYCLE):
                try:
                    if advance_random_shipment(conn) is not None:
                        updated += 1
                except Exception as exc:
                    conn.rollback()
                    print(f"[ERROR SHIPMENT STREAM] update: {exc}")

            print(
                f"[SHIPMENT CYCLE] created={created}/{SHIPMENTS_CREATED_PER_CYCLE}, "
                f"updated={updated}/{SHIPMENT_UPDATES_PER_CYCLE}"
            )
            sleep_until_next_cycle(cycle_started_at, STREAM_INTERVAL_SECONDS)
    finally:
        conn.close()


if __name__ == "__main__":
    run_shipment_stream()
