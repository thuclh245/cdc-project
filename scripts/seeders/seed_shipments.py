from scripts.services.shipment_service import insert_shipment_for_paid_order


def seed_shipments(conn, minimum=1):
    existing = table_count(conn, "shipments")
    missing = max(0, minimum - existing)
    if missing == 0:
        return

    print(f"Seeding {missing} shipments...")
    created = 0
    for _ in range(missing):
        shipment_id = insert_shipment_for_paid_order(conn)
        if shipment_id is None:
            raise RuntimeError("cannot seed shipments: no paid order without shipment")
        created += 1

    print(f"Seed shipments done: {created}")


def table_count(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]
