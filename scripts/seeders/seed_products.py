from scripts.services.product_service import insert_product


def seed_products(conn, total=300):
    print(f"Seeding {total} products...")

    for index in range(1, total + 1):
        insert_product(conn, log=False)
        if index % 100 == 0 or index == total:
            print(f"  products: {index}/{total}")

    print("Seed products done.")
