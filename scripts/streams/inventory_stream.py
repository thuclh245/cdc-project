import time

from scripts.common.constants import INVENTORY_IMPORTS_PER_CYCLE, INVENTORY_INTERVAL_SECONDS
from scripts.common.utils import sleep_until_next_cycle
from scripts.database.connection import get_conn
from scripts.services.inventory_service import import_random_stock


def run_inventory_stream():
    conn = get_conn()

    try:
        print(
            "Inventory stream started: "
            f"{INVENTORY_IMPORTS_PER_CYCLE} stock import every "
            f"{INVENTORY_INTERVAL_SECONDS}s."
        )

        while True:
            cycle_started_at = time.monotonic()
            imported = 0

            for _ in range(INVENTORY_IMPORTS_PER_CYCLE):
                try:
                    if import_random_stock(conn) is not None:
                        imported += 1
                except Exception as exc:
                    conn.rollback()
                    print(f"[ERROR INVENTORY STREAM] import: {exc}")

            print(
                f"[INVENTORY CYCLE] imported={imported}/{INVENTORY_IMPORTS_PER_CYCLE}"
            )
            sleep_until_next_cycle(cycle_started_at, INVENTORY_INTERVAL_SECONDS)
    finally:
        conn.close()


if __name__ == "__main__":
    run_inventory_stream()
