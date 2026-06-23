from scripts.services.customer_service import insert_customer


def seed_customers(conn, total=1000):
    print(f"Seeding {total} customers...")

    for _ in range(total):
        insert_customer(conn)

    print("Seed customers done.")