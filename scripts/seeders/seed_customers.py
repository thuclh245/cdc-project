from scripts.services.customer_service import insert_customer


def seed_customers(conn, total=1000):
    print(f"Seeding {total} customers...")

    for index in range(1, total + 1):
        insert_customer(conn, log=False)
        if index % 100 == 0 or index == total:
            print(f"  customers: {index}/{total}")

    print("Seed customers done.")
