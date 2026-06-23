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

        if customer_count == 0:
            seed_customers(conn, SEED_CUSTOMERS)

        if product_count == 0:
            seed_products(conn, SEED_PRODUCTS)

        if order_count == 0:
            seed_orders(conn, SEED_ORDERS)

        print("Seed completed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()