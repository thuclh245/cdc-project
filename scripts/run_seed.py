from scripts.database.connection import get_conn
from scripts.common.constants import SEED_CUSTOMERS, SEED_PRODUCTS, SEED_ORDERS
from scripts.seeders.seed_customers import seed_customers
from scripts.seeders.seed_products import seed_products
from scripts.seeders.seed_orders import seed_orders


def table_count(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]


def main():
    conn = get_conn()

    try:
        customer_count = table_count(conn, "customers")
        product_count = table_count(conn, "products")
        order_count = table_count(conn, "orders")

        print("Current data:")
        print(f"customers = {customer_count}")
        print(f"products  = {product_count}")
        print(f"orders    = {order_count}")

        missing_customers = max(0, SEED_CUSTOMERS - customer_count)
        missing_products = max(0, SEED_PRODUCTS - product_count)
        missing_orders = max(0, SEED_ORDERS - order_count)

        if missing_customers:
            seed_customers(conn, missing_customers)

        if missing_products:
            seed_products(conn, missing_products)

        if missing_orders:
            seed_orders(conn, missing_orders)

        print("Final data:")
        print(f"customers = {table_count(conn, 'customers')}")
        print(f"products  = {table_count(conn, 'products')}")
        print(f"orders    = {table_count(conn, 'orders')}")

        print("Seed completed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
