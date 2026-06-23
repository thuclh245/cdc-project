import random
import uuid

from scripts.common.constants import SHIPMENT_CARRIERS
from scripts.common.utils import log_event


NEXT_SHIPMENT_STATUS = {
    "PENDING": "PICKED_UP",
    "PICKED_UP": "IN_TRANSIT",
    "IN_TRANSIT": "DELIVERED",
}


def insert_shipment_for_paid_order(conn):
    """Create at most one shipment for an eligible paid order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                o.order_id,
                o.shipping_address,
                o.shipping_city,
                o.shipping_country
            FROM orders o
            WHERE o.payment_status = 'PAID'
              AND o.deleted_at IS NULL
              AND o.order_status != 'CANCELLED'
              AND NOT EXISTS (
                  SELECT 1
                  FROM shipments s
                  WHERE s.order_id = o.order_id
              )
            ORDER BY random()
            LIMIT 1
            FOR UPDATE OF o SKIP LOCKED
            """
        )
        order = cur.fetchone()

        if order is None:
            return None

        order_id, address, city, country = order
        carrier = random.choice(SHIPMENT_CARRIERS)
        tracking_number = f"VN-{uuid.uuid4().hex[:16].upper()}"

        cur.execute(
            """
            INSERT INTO shipments (
                order_id,
                carrier,
                tracking_number,
                shipment_status,
                shipping_address,
                shipping_city,
                shipping_country
            )
            VALUES (%s, %s, %s, 'PENDING', %s, %s, %s)
            RETURNING shipment_id
            """,
            (order_id, carrier, tracking_number, address, city, country),
        )
        shipment_id = cur.fetchone()[0]

    conn.commit()
    log_event(
        "INSERT SHIPMENT",
        f"shipment_id={shipment_id}, order_id={order_id}, carrier={carrier}",
    )
    return shipment_id


def advance_random_shipment(conn):
    """Advance one shipment through its lifecycle without skipping states."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shipment_id, order_id, shipment_status
            FROM shipments
            WHERE deleted_at IS NULL
              AND shipment_status IN ('PENDING', 'PICKED_UP', 'IN_TRANSIT')
              AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '1 minute'
            ORDER BY random()
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )
        shipment = cur.fetchone()

        if shipment is None:
            return None

        shipment_id, order_id, old_status = shipment
        new_status = NEXT_SHIPMENT_STATUS[old_status]

        cur.execute(
            """
            UPDATE shipments
            SET shipment_status = %s,
                shipped_at = CASE
                    WHEN %s = 'PICKED_UP' THEN COALESCE(shipped_at, CURRENT_TIMESTAMP)
                    ELSE shipped_at
                END,
                delivered_at = CASE
                    WHEN %s = 'DELIVERED' THEN CURRENT_TIMESTAMP
                    ELSE delivered_at
                END
            WHERE shipment_id = %s
            """,
            (new_status, new_status, new_status, shipment_id),
        )

        if new_status in {"PICKED_UP", "IN_TRANSIT"}:
            cur.execute(
                """
                UPDATE orders
                SET order_status = 'SHIPPING'
                WHERE order_id = %s
                  AND deleted_at IS NULL
                  AND order_status != 'CANCELLED'
                """,
                (order_id,),
            )
        elif new_status == "DELIVERED":
            cur.execute(
                """
                UPDATE orders
                SET order_status = 'COMPLETED'
                WHERE order_id = %s
                  AND deleted_at IS NULL
                """,
                (order_id,),
            )

    conn.commit()
    log_event(
        "UPDATE SHIPMENT",
        f"shipment_id={shipment_id}, {old_status}->{new_status}",
    )
    return shipment_id
