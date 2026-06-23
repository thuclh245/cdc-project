import random
from decimal import Decimal
from faker import Faker

from scripts.common.constants import ORDER_STATUSES
from scripts.common.utils import (
    random_discount,
    random_shipping_fee,
    log_event,
)
from scripts.services.customer_service import get_random_customer_id, insert_customer
from scripts.services.product_service import (
    get_random_product,
    get_product_by_id,
    insert_product,
    decrease_stock,
)
from scripts.services.payment_service import (
    insert_payment,
    update_order_payment_status,
)


fake = Faker("vi_VN")


def insert_order(conn):
    customer_id = get_random_customer_id(conn)

    if customer_id is None:
        customer_id = insert_customer(conn)

    item_count = random.randint(1, 5)
    order_items = []
    total_amount = Decimal("0.00")

    for _ in range(item_count):
        product = get_random_product(conn)

        if product is None:
            product_id = insert_product(conn)
            product = get_product_by_id(conn, product_id)

        product_id, unit_price = product

        quantity = random.randint(1, 5)
        discount_amount = random_discount()
        item_total = Decimal(unit_price) * quantity - discount_amount

        if item_total < 0:
            item_total = Decimal("0.00")

        order_items.append(
            {
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount_amount,
                "total_amount": item_total,
            }
        )

        total_amount += item_total

    shipping_fee = random_shipping_fee()
    order_discount = random_discount()

    final_amount = total_amount + shipping_fee - order_discount

    if final_amount < 0:
        final_amount = Decimal("0.00")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders (
                customer_id,
                order_status,
                order_date,
                total_amount,
                discount_amount,
                shipping_fee,
                final_amount,
                payment_status,
                shipping_address,
                shipping_city,
                shipping_country
            )
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING order_id
            """,
            (
                customer_id,
                random.choice(["PENDING", "CONFIRMED"]),
                total_amount,
                order_discount,
                shipping_fee,
                final_amount,
                "UNPAID",
                fake.address(),
                fake.city(),
                "Vietnam",
            ),
        )

        order_id = cur.fetchone()[0]

        for item in order_items:
            cur.execute(
                """
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount_amount,
                    total_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    item["product_id"],
                    item["quantity"],
                    item["unit_price"],
                    item["discount_amount"],
                    item["total_amount"],
                ),
            )

            decrease_stock(conn, item["product_id"], item["quantity"])

        if random.random() < 0.8:
            payment_id, payment_status = insert_payment(conn, order_id, final_amount)
            update_order_payment_status(conn, order_id, payment_status)

    conn.commit()
    log_event("INSERT ORDER", f"order_id={order_id}, final_amount={final_amount}")
    return order_id


def update_random_order(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT order_id, order_status
            FROM orders
            WHERE deleted_at IS NULL
              AND order_status != 'CANCELLED'
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

        if row is None:
            return None

        order_id, old_status = row
        new_status = random.choice(ORDER_STATUSES)

        cur.execute(
            """
            UPDATE orders
            SET order_status = %s
            WHERE order_id = %s
            """,
            (new_status, order_id),
        )

    conn.commit()
    log_event("UPDATE ORDER", f"order_id={order_id}, {old_status}->{new_status}")
    return order_id


def soft_delete_random_order(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT order_id
            FROM orders
            WHERE deleted_at IS NULL
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cur.fetchone()

        if row is None:
            return None

        order_id = row[0]

        cur.execute(
            """
            UPDATE orders
            SET deleted_at = CURRENT_TIMESTAMP,
                order_status = 'CANCELLED'
            WHERE order_id = %s
            """,
            (order_id,),
        )

        cur.execute(
            """
            UPDATE order_items
            SET deleted_at = CURRENT_TIMESTAMP
            WHERE order_id = %s
            """,
            (order_id,),
        )

        cur.execute(
            """
            UPDATE payments
            SET deleted_at = CURRENT_TIMESTAMP,
                payment_status = 'REFUNDED'
            WHERE order_id = %s
              AND payment_status = 'SUCCESS'
            """,
            (order_id,),
        )

    conn.commit()
    log_event("SOFT DELETE ORDER", f"order_id={order_id}")
    return order_id