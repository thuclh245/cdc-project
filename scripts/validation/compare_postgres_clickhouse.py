"""Compare key PostgreSQL metrics with their ClickHouse CDC copies.

Run from the project root with:
    python -m scripts.validation.compare_postgres_clickhouse
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.database.connection import get_conn


@dataclass(frozen=True)
class Check:
    name: str
    postgres_sql: str
    clickhouse_sql: str
    parser: Callable[[str], object]


def parse_int(value: str) -> int:
    return int(value.strip())


def parse_decimal(value: str) -> Decimal:
    try:
        return Decimal(value.strip())
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal returned by ClickHouse: {value!r}") from exc


def parse_text(value: str) -> str:
    return value.strip()


CHECKS = (
    Check(
        "table active/deleted counts",
        """
        WITH metrics AS (
            SELECT 'categories' AS table_name, COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE deleted_at IS NULL) AS active,
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted FROM categories
            UNION ALL SELECT 'customers', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM customers
            UNION ALL SELECT 'inventory_movements', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM inventory_movements
            UNION ALL SELECT 'order_items', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM order_items
            UNION ALL SELECT 'orders', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM orders
            UNION ALL SELECT 'payments', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM payments
            UNION ALL SELECT 'products', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM products
            UNION ALL SELECT 'shipments', COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL),
                   COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) FROM shipments
        )
        SELECT string_agg(table_name || ':' || total || ':' || active || ':' || deleted, ',' ORDER BY table_name)
        FROM metrics
        """,
        """
        SELECT arrayStringConcat(groupArray(concat(table_name, ':', toString(total), ':', toString(active), ':', toString(deleted))), ',')
        FROM (
            SELECT table_name, total, active, deleted
            FROM (
                SELECT 'categories' AS table_name, count() AS total, countIf(deleted_at IS NULL) AS active,
                       countIf(deleted_at IS NOT NULL) AS deleted FROM ecommerce_ods.categories_sink FINAL
                UNION ALL SELECT 'customers', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.customers_sink FINAL
                UNION ALL SELECT 'inventory_movements', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.inventory_movements_sink FINAL
                UNION ALL SELECT 'order_items', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.order_items_sink FINAL
                UNION ALL SELECT 'orders', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.orders_sink FINAL
                UNION ALL SELECT 'payments', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.payments_sink FINAL
                UNION ALL SELECT 'products', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.products_sink FINAL
                UNION ALL SELECT 'shipments', count(), countIf(deleted_at IS NULL),
                       countIf(deleted_at IS NOT NULL) FROM ecommerce_ods.shipments_sink FINAL
            )
            ORDER BY table_name
        )
        """,
        parse_text,
    ),
    Check(
        "orders revenue",
        "SELECT COALESCE(SUM(final_amount), 0) FROM orders WHERE deleted_at IS NULL",
        "SELECT coalesce(sum(final_amount), 0) FROM ecommerce_ods.orders_sink FINAL WHERE deleted_at IS NULL",
        parse_decimal,
    ),
    Check(
        "payments total",
        "SELECT COALESCE(SUM(payment_amount), 0) FROM payments WHERE deleted_at IS NULL",
        "SELECT coalesce(sum(payment_amount), 0) FROM ecommerce_ods.payments_sink FINAL WHERE deleted_at IS NULL",
        parse_decimal,
    ),
    Check(
        "product stock total",
        "SELECT COALESCE(SUM(stock_quantity), 0) FROM products WHERE deleted_at IS NULL",
        "SELECT coalesce(sum(stock_quantity), 0) FROM ecommerce_ods.products_sink FINAL WHERE deleted_at IS NULL",
        parse_int,
    ),
    Check(
        "shipment status counts",
        """
        SELECT COALESCE(string_agg(shipment_status || ':' || cnt, ',' ORDER BY shipment_status), '')
        FROM (
            SELECT shipment_status, COUNT(*) AS cnt
            FROM shipments
            WHERE deleted_at IS NULL
            GROUP BY shipment_status
        ) s
        """,
        """
        SELECT coalesce(arrayStringConcat(groupArray(concat(shipment_status, ':', toString(cnt))), ','), '')
        FROM (
            SELECT shipment_status, count() AS cnt
            FROM ecommerce_ods.shipments_sink FINAL
            WHERE deleted_at IS NULL
            GROUP BY shipment_status
            ORDER BY shipment_status
        )
        """,
        parse_text,
    ),
    Check(
        "inventory movement type counts",
        """
        SELECT COALESCE(string_agg(movement_type || ':' || cnt, ',' ORDER BY movement_type), '')
        FROM (
            SELECT movement_type, COUNT(*) AS cnt
            FROM inventory_movements
            WHERE deleted_at IS NULL
            GROUP BY movement_type
        ) m
        """,
        """
        SELECT coalesce(arrayStringConcat(groupArray(concat(movement_type, ':', toString(cnt))), ','), '')
        FROM (
            SELECT movement_type, count() AS cnt
            FROM ecommerce_ods.inventory_movements_sink FINAL
            WHERE deleted_at IS NULL
            GROUP BY movement_type
            ORDER BY movement_type
        )
        """,
        parse_text,
    ),
    Check(
        "inventory movement quantity",
        "SELECT COALESCE(SUM(quantity_change), 0) FROM inventory_movements WHERE deleted_at IS NULL",
        "SELECT coalesce(sum(quantity_change), 0) FROM ecommerce_ods.inventory_movements_sink FINAL WHERE deleted_at IS NULL",
        parse_int,
    ),
    Check(
        "orphan foreign keys",
        """
        SELECT
            (
                SELECT COUNT(*) FROM orders o
                LEFT JOIN customers c ON c.customer_id = o.customer_id
                WHERE o.deleted_at IS NULL AND c.customer_id IS NULL
            ) +
            (
                SELECT COUNT(*) FROM order_items oi
                LEFT JOIN orders o ON o.order_id = oi.order_id
                LEFT JOIN products p ON p.product_id = oi.product_id
                WHERE oi.deleted_at IS NULL AND (o.order_id IS NULL OR p.product_id IS NULL)
            ) +
            (
                SELECT COUNT(*) FROM payments p
                LEFT JOIN orders o ON o.order_id = p.order_id
                WHERE p.deleted_at IS NULL AND o.order_id IS NULL
            ) +
            (
                SELECT COUNT(*) FROM shipments s
                LEFT JOIN orders o ON o.order_id = s.order_id
                WHERE s.deleted_at IS NULL AND o.order_id IS NULL
            ) +
            (
                SELECT COUNT(*) FROM inventory_movements im
                LEFT JOIN products p ON p.product_id = im.product_id
                LEFT JOIN orders o ON o.order_id = im.order_id
                WHERE im.deleted_at IS NULL
                  AND (p.product_id IS NULL OR (im.order_id IS NOT NULL AND o.order_id IS NULL))
            )
        """,
        """
        SELECT
            (
                SELECT count() FROM (SELECT * FROM ecommerce_ods.orders_sink FINAL) AS o
                LEFT JOIN (SELECT * FROM ecommerce_ods.customers_sink FINAL) AS c ON c.customer_id = o.customer_id
                WHERE o.deleted_at IS NULL AND c.customer_id = 0
            ) +
            (
                SELECT count() FROM (SELECT * FROM ecommerce_ods.order_items_sink FINAL) AS oi
                LEFT JOIN (SELECT * FROM ecommerce_ods.orders_sink FINAL) AS o ON o.order_id = oi.order_id
                LEFT JOIN (SELECT * FROM ecommerce_ods.products_sink FINAL) AS p ON p.product_id = oi.product_id
                WHERE oi.deleted_at IS NULL AND (o.order_id = 0 OR p.product_id = 0)
            ) +
            (
                SELECT count() FROM (SELECT * FROM ecommerce_ods.payments_sink FINAL) AS p
                LEFT JOIN (SELECT * FROM ecommerce_ods.orders_sink FINAL) AS o ON o.order_id = p.order_id
                WHERE p.deleted_at IS NULL AND o.order_id = 0
            ) +
            (
                SELECT count() FROM (SELECT * FROM ecommerce_ods.shipments_sink FINAL) AS s
                LEFT JOIN (SELECT * FROM ecommerce_ods.orders_sink FINAL) AS o ON o.order_id = s.order_id
                WHERE s.deleted_at IS NULL AND o.order_id = 0
            ) +
            (
                SELECT count() FROM (SELECT * FROM ecommerce_ods.inventory_movements_sink FINAL) AS im
                LEFT JOIN (SELECT * FROM ecommerce_ods.products_sink FINAL) AS p ON p.product_id = im.product_id
                LEFT JOIN (SELECT * FROM ecommerce_ods.orders_sink FINAL) AS o ON o.order_id = im.order_id
                WHERE im.deleted_at IS NULL
                  AND (p.product_id = 0 OR (im.order_id IS NOT NULL AND o.order_id = 0))
            )
        """,
        parse_int,
    ),
    Check(
        "updated row counts",
        """
        WITH metrics AS (
            SELECT 'orders' AS table_name, COUNT(*) AS updated_rows
            FROM orders
            WHERE updated_at > created_at
            UNION ALL SELECT 'payments', COUNT(*)
            FROM payments
            WHERE updated_at > created_at
            UNION ALL SELECT 'shipments', COUNT(*)
            FROM shipments
            WHERE updated_at > created_at
            UNION ALL SELECT 'inventory_movements', COUNT(*)
            FROM inventory_movements
            WHERE updated_at > created_at
        )
        SELECT string_agg(table_name || ':' || updated_rows, ',' ORDER BY table_name)
        FROM metrics
        """,
        """
        SELECT arrayStringConcat(groupArray(concat(table_name, ':', toString(updated_rows))), ',')
        FROM (
            SELECT table_name, updated_rows
            FROM (
                SELECT 'orders' AS table_name, count() AS updated_rows
                FROM ecommerce_ods.orders_sink FINAL
                WHERE updated_at > created_at
                UNION ALL SELECT 'payments', count()
                FROM ecommerce_ods.payments_sink FINAL
                WHERE updated_at > created_at
                UNION ALL SELECT 'shipments', count()
                FROM ecommerce_ods.shipments_sink FINAL
                WHERE updated_at > created_at
                UNION ALL SELECT 'inventory_movements', count()
                FROM ecommerce_ods.inventory_movements_sink FINAL
                WHERE updated_at > created_at
            )
            ORDER BY table_name
        )
        """,
        parse_text,
    ),
)


class ClickHouseHttpClient:
    def __init__(self) -> None:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = os.getenv("CLICKHOUSE_PORT", "8123")
        self.url = f"http://{host}:{port}/?{urlencode({'database': os.getenv('CLICKHOUSE_DB', 'ecommerce_ods')})}"
        self.user = os.getenv("CLICKHOUSE_USER", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.timeout = float(os.getenv("VALIDATION_TIMEOUT", "10"))

    def scalar(self, sql: str, parser: Callable[[str], object]) -> object:
        request = Request(
            self.url,
            data=f"{sql} FORMAT TabSeparatedRaw".encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self.user,
                "X-ClickHouse-Key": self.password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                value = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"cannot connect to ClickHouse at {self.url}: {exc.reason}") from exc
        return parser(value)


def postgres_scalar(conn, sql: str):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()[0]


def display_value(value: object) -> str:
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    return str(value)


def run_checks_once() -> list[tuple[Check, object, object]]:
    clickhouse = ClickHouseHttpClient()
    mismatches = []
    postgres = get_conn()

    try:
        for check in CHECKS:
            pg_value = postgres_scalar(postgres, check.postgres_sql)
            ch_value = clickhouse.scalar(check.clickhouse_sql, check.parser)

            if pg_value != ch_value:
                mismatches.append((check, pg_value, ch_value))
    finally:
        postgres.close()

    return mismatches


def run_checks() -> bool:
    timeout = float(os.getenv("VALIDATION_POLL_TIMEOUT", "30"))
    interval = float(os.getenv("VALIDATION_POLL_INTERVAL", "3"))
    deadline = time.monotonic() + timeout
    attempt = 0
    last_mismatches: list[tuple[Check, object, object]] = []

    while True:
        attempt += 1
        last_mismatches = run_checks_once()
        if not last_mismatches:
            break
        if time.monotonic() >= deadline:
            break
        print(
            f"[WAIT] {len(last_mismatches)}/{len(CHECKS)} checks still catching up "
            f"(attempt {attempt}); retrying in {interval:g}s..."
        )
        time.sleep(interval)

    failed_names = {check.name for check, _, _ in last_mismatches}
    for check in CHECKS:
        mismatch = next(
            (
                (pg_value, ch_value)
                for failed_check, pg_value, ch_value in last_mismatches
                if failed_check.name == check.name
            ),
            None,
        )
        if mismatch is None:
            print(f"[PASS] {check.name}")
        else:
            pg_value, ch_value = mismatch
            print(
                f"[FAIL] {check.name}: "
                f"postgres={display_value(pg_value)} "
                f"clickhouse={display_value(ch_value)}"
            )

    passed = len(CHECKS) - len(failed_names)
    print(f"\n{passed}/{len(CHECKS)} PASS")
    return not last_mismatches


def main() -> int:
    try:
        matched = run_checks()
    except Exception as exc:
        detail = str(exc) or exc.__class__.__name__
        print(f"[ERROR] validation could not run: {detail}", file=sys.stderr)
        return 2

    if matched:
        print("\nValidation passed: PostgreSQL and ClickHouse are consistent.")
        return 0

    print("\nValidation failed: one or more checks did not match.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
