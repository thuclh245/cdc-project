import os

import psycopg2


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "15432")),
    "database": os.getenv("POSTGRES_DB", "ecommerce_ods"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10")),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)
