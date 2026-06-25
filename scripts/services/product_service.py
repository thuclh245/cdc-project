import random
from decimal import Decimal
from faker import Faker

from scripts.common.constants import CATEGORIES, BRANDS
from scripts.common.utils import random_money, log_event
from scripts.services.inventory_service import record_inventory_movement


fake = Faker("vi_VN")


def insert_product(conn, *, log=True):
    price = random_money()
    cost = (price * Decimal(str(random.uniform(0.5, 0.8)))).quantize(Decimal("0.01"))
    initial_stock = random.randint(20, 1000)

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
                initial_stock,
                "ACTIVE",
            ),
        )

        product_id = cur.fetchone()[0]

    record_inventory_movement(
        conn,
        product_id=product_id,
        order_id=None,
        movement_type="IMPORT",
        quantity_change=initial_stock,
        old_stock=0,
        new_stock=initial_stock,
        reason="Initial stock for new product",
    )
    conn.commit()
    if log:
        log_event("INSERT PRODUCT", f"product_id={product_id}")
    return product_id


def get_random_product(conn, minimum_stock=1, excluded_product_ids=None):
    """Lock and return one sellable product with enough stock.

    The lock is held until the caller commits or rolls back, so an order can
    validate and reserve inventory in the same transaction.
    """
    excluded_product_ids = excluded_product_ids or []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, price
            FROM products
            WHERE deleted_at IS NULL
              AND product_status = 'ACTIVE'
              AND stock_quantity >= %s
              AND NOT (product_id = ANY(%s::BIGINT[]))
            ORDER BY random()
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """,
            (minimum_stock, excluded_product_ids),
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


def update_random_product(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, price, stock_quantity
            FROM products
            WHERE deleted_at IS NULL
            ORDER BY random()
            LIMIT 1
            FOR UPDATE SKIP LOCKED
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

            quantity_change = new_stock - old_stock
            if quantity_change != 0:
                record_inventory_movement(
                    conn,
                    product_id=product_id,
                    order_id=None,
                    movement_type="ADJUSTMENT",
                    quantity_change=quantity_change,
                    old_stock=old_stock,
                    new_stock=new_stock,
                    reason="Stock corrected by product stream",
                )

            message = f"product_id={product_id}, stock={old_stock}->{new_stock}"

    conn.commit()
    log_event("UPDATE PRODUCT", message)
    return product_id
