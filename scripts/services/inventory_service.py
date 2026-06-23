import random

from scripts.common.utils import log_event


MOVEMENT_TYPES = {"IMPORT", "SALE", "RETURN", "ADJUSTMENT", "CANCEL_ORDER"}


def record_inventory_movement(
    conn,
    *,
    product_id,
    order_id,
    movement_type,
    quantity_change,
    old_stock,
    new_stock,
    reason,
):
    """Write an inventory audit row inside the caller's transaction."""
    if movement_type not in MOVEMENT_TYPES:
        raise ValueError(f"unsupported inventory movement type: {movement_type}")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO inventory_movements (
                product_id,
                order_id,
                movement_type,
                quantity_change,
                old_stock,
                new_stock,
                reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING movement_id
            """,
            (
                product_id,
                order_id,
                movement_type,
                quantity_change,
                old_stock,
                new_stock,
                reason,
            ),
        )
        return cur.fetchone()[0]


def decrease_stock_for_sale(conn, product_id, quantity, order_id):
    """Atomically reserve stock and record a SALE movement."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE products
            SET stock_quantity = stock_quantity - %s,
                product_status = CASE
                    WHEN stock_quantity - %s = 0 THEN 'OUT_OF_STOCK'
                    ELSE 'ACTIVE'
                END
            WHERE product_id = %s
              AND deleted_at IS NULL
              AND stock_quantity >= %s
            RETURNING stock_quantity + %s, stock_quantity
            """,
            (quantity, quantity, product_id, quantity, quantity),
        )
        stock = cur.fetchone()

    if stock is None:
        raise ValueError(
            f"insufficient stock for product_id={product_id}, quantity={quantity}"
        )

    old_stock, new_stock = stock
    record_inventory_movement(
        conn,
        product_id=product_id,
        order_id=order_id,
        movement_type="SALE",
        quantity_change=-quantity,
        old_stock=old_stock,
        new_stock=new_stock,
        reason=f"Stock sold for order {order_id}",
    )


def restore_stock_for_cancelled_order(conn, order_id):
    """Restore every product in an order and record CANCEL_ORDER movements."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, SUM(quantity)::INT
            FROM order_items
            WHERE order_id = %s
              AND deleted_at IS NULL
            GROUP BY product_id
            """,
            (order_id,),
        )
        items = cur.fetchall()

    for product_id, quantity in items:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE products
                SET stock_quantity = stock_quantity + %s,
                    product_status = 'ACTIVE'
                WHERE product_id = %s
                RETURNING stock_quantity - %s, stock_quantity
                """,
                (quantity, product_id, quantity),
            )
            old_stock, new_stock = cur.fetchone()

        record_inventory_movement(
            conn,
            product_id=product_id,
            order_id=order_id,
            movement_type="CANCEL_ORDER",
            quantity_change=quantity,
            old_stock=old_stock,
            new_stock=new_stock,
            reason=f"Stock restored after cancelling order {order_id}",
        )


def import_random_stock(conn):
    """Import stock for one random product and commit it as one unit of work."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_id, stock_quantity
            FROM products
            WHERE deleted_at IS NULL
              AND product_status != 'DISCONTINUED'
            ORDER BY random()
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )
        product = cur.fetchone()

        if product is None:
            return None

        product_id, old_stock = product
        quantity = random.randint(20, 100)
        new_stock = old_stock + quantity

        cur.execute(
            """
            UPDATE products
            SET stock_quantity = %s,
                product_status = 'ACTIVE'
            WHERE product_id = %s
            """,
            (new_stock, product_id),
        )

    movement_id = record_inventory_movement(
        conn,
        product_id=product_id,
        order_id=None,
        movement_type="IMPORT",
        quantity_change=quantity,
        old_stock=old_stock,
        new_stock=new_stock,
        reason="Scheduled warehouse stock import",
    )
    conn.commit()
    log_event(
        "IMPORT STOCK",
        f"movement_id={movement_id}, product_id={product_id}, +{quantity}",
    )
    return movement_id
