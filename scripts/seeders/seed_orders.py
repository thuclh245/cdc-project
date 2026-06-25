from scripts.services.order_service import insert_order
from scripts.services.inventory_service import InsufficientStockError, import_random_stock


def seed_orders(conn, total=1000):
    print(f"Seeding {total} orders...")

    created = 0
    while created < total:
        try:
            insert_order(conn, log=False)
        except InsufficientStockError:
            conn.rollback()
            if import_random_stock(conn) is None:
                raise RuntimeError("cannot seed orders: no product can be restocked")
            continue

        created += 1
        if created % 100 == 0 or created == total:
            print(f"  orders: {created}/{total}")

    print("Seed orders done.")
