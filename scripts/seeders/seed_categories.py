from scripts.common.constants import CATEGORIES


def seed_categories(conn):
    print(f"Seeding {len(CATEGORIES)} categories...")

    with conn.cursor() as cur:
        for category in CATEGORIES:
            cur.execute(
                """
                INSERT INTO categories (
                    category_name,
                    description,
                    category_status
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (category_name) DO NOTHING
                """,
                (
                    category,
                    f"{category} products",
                    "ACTIVE",
                ),
            )

    conn.commit()
    print("Seed categories done.")
