import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ecommerce_ods",
    "user": "postgres",
    "password": "postgres",
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)