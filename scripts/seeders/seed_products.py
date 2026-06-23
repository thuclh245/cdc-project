from scripts.services.product_service import insert_product


def seed_products(conn, total=300):
    print(f"Seeding {total} products...")

    for _ in range(total):
        insert_product(conn)

    print("Seed products done.")