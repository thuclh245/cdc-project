import random
from decimal import Decimal
from faker import Faker

from scripts.common.constants import CATEGORIES, BRANDS
from scripts.common.utils import random_money, log_event


fake = Faker("vi_VN")


def insert_product(conn):
    price = random_money()
    cost = (price * Decimal(str(random.uniform(0.5, 0.8)))).quantize(Decimal("0.01"))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO products (
                product_name,
                category,
                brand,
                price,
                cost,
                stock_quantity,
                product_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING product_id
            """,
            (
                fake.word().capitalize() + " " + random.choice(["Pro", "Max", "Plus", "Lite", "2026"]),
                random.choice(CATEGORIES),
                random.choice(BRANDS),
                price,
                cost,
                random.randint(20, 1000),
                "ACTIVE",
            ),
        )

        product_id = cur.fetchone()[0]

    conn.commit()
    log_event("INSERT PRODUCT", f"product_id={product_id}")
    return product_id


def get_random_product(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, price
            FROM products
            WHERE deleted_at IS NULL
              AND product_status = 'ACTIVE'
              AND stock_quantity > 0
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

    return row


def get_product_by_id(conn, product_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, price
            FROM products
            WHERE product_id = %s
            """,
            (product_id,),
        )

        row = cur.fetchone()

    return row


def decrease_stock(conn, product_id, quantity):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE products
            SET stock_quantity = GREATEST(stock_quantity - %s, 0),
                product_status = CASE
                    WHEN GREATEST(stock_quantity - %s, 0) = 0 THEN 'OUT_OF_STOCK'
                    ELSE 'ACTIVE'
                END
            WHERE product_id = %s
            """,
            (quantity, quantity, product_id),
        )


def update_random_product(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, price, stock_quantity
            FROM products
            WHERE deleted_at IS NULL
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

        if row is None:
            return None

        product_id, old_price, old_stock = row

        update_type = random.choice(["PRICE", "STOCK"])

        if update_type == "PRICE":
            new_price = random_money()
            cur.execute(
                """
                UPDATE products
                SET price = %s
                WHERE product_id = %s
                """,
                (new_price, product_id),
            )

            message = f"product_id={product_id}, price={old_price}->{new_price}"

        else:
            new_stock = max(0, old_stock + random.randint(-10, 50))
            product_status = "OUT_OF_STOCK" if new_stock == 0 else "ACTIVE"

            cur.execute(
                """
                UPDATE products
                SET stock_quantity = %s,
                    product_status = %s
                WHERE product_id = %s
                """,
                (new_stock, product_status, product_id),
            )

            message = f"product_id={product_id}, stock={old_stock}->{new_stock}"

    conn.commit()
    log_event("UPDATE PRODUCT", message)
    return product_id