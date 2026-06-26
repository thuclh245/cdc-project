from scripts.database.connection import get_conn
from scripts.common.constants import SEED_CATEGORIES, SEED_CUSTOMERS, SEED_PRODUCTS, SEED_ORDERS
from scripts.seeders.seed_categories import seed_categories
from scripts.seeders.seed_customers import seed_customers
from scripts.seeders.seed_products import seed_products
from scripts.seeders.seed_orders import seed_orders
from scripts.seeders.seed_shipments import seed_shipments


CDC_TABLES = (
    "customers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory_movements",
)


def table_count(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]


def table_counts(conn):
    return {table: table_count(conn, table) for table in CDC_TABLES}


def print_counts(title, counts):
    print(title)
    for table in CDC_TABLES:
        print(f"{table:19} = {counts[table]}")


def verify_seeded_cdc_tables(counts):
    empty_tables = [table for table, count in counts.items() if count == 0]
    if empty_tables:
        raise RuntimeError(
            "CDC source tables must not be empty after seed: "
            + ", ".join(empty_tables)
        )
    print("All CDC source tables contain seed data.")


def main():
    conn = get_conn()

    try:
        counts = table_counts(conn)
        print_counts("Current data:", counts)

        missing_customers = max(0, SEED_CUSTOMERS - counts["customers"])
        missing_categories = max(0, SEED_CATEGORIES - counts["categories"])
        missing_products = max(0, SEED_PRODUCTS - counts["products"])
        missing_orders = max(0, SEED_ORDERS - counts["orders"])

        if missing_customers:
            seed_customers(conn, missing_customers)

        if missing_categories:
            seed_categories(conn)

        if missing_products:
            seed_products(conn, missing_products)

        if missing_orders:
            seed_orders(conn, missing_orders)

        seed_shipments(conn)

        final_counts = table_counts(conn)
        print_counts("Final data:", final_counts)
        verify_seeded_cdc_tables(final_counts)

        print("Seed completed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
