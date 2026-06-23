from scripts.services.order_service import insert_order


def seed_orders(conn, total=1000):
    print(f"Seeding {total} orders...")

    for _ in range(total):
        insert_order(conn)

    print("Seed orders done.")