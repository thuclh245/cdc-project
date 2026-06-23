"""Compare key PostgreSQL metrics with their ClickHouse CDC copies.

Run from the project root with:
    python -m scripts.validation.compare_postgres_clickhouse
"""

from __future__ import annotations

import os
import sys
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


CHECKS = (
    Check(
        "active_orders",
        "SELECT COUNT(*) FROM orders WHERE deleted_at IS NULL",
        "SELECT count() FROM ecommerce_ods.orders_sink FINAL "
        "WHERE deleted_at IS NULL",
        parse_int,
    ),
    Check(
        "revenue",
        "SELECT COALESCE(SUM(final_amount), 0) FROM orders "
        "WHERE deleted_at IS NULL",
        "SELECT coalesce(sum(final_amount), 0) "
        "FROM ecommerce_ods.orders_sink FINAL WHERE deleted_at IS NULL",
        parse_decimal,
    ),
    Check(
        "customers count",
        "SELECT COUNT(*) FROM customers",
        "SELECT count() FROM ecommerce_ods.customers_sink FINAL",
        parse_int,
    ),
    Check(
        "products count",
        "SELECT COUNT(*) FROM products",
        "SELECT count() FROM ecommerce_ods.products_sink FINAL",
        parse_int,
    ),
    Check(
        "order_items count",
        "SELECT COUNT(*) FROM order_items",
        "SELECT count() FROM ecommerce_ods.order_items_sink FINAL",
        parse_int,
    ),
    Check(
        "payments count",
        "SELECT COUNT(*) FROM payments",
        "SELECT count() FROM ecommerce_ods.payments_sink FINAL",
        parse_int,
    ),
    Check(
        "shipments count",
        "SELECT COUNT(*) FROM shipments",
        "SELECT count() FROM ecommerce_ods.shipments_sink FINAL",
        parse_int,
    ),
    Check(
        "inventory_movements count",
        "SELECT COUNT(*) FROM inventory_movements",
        "SELECT count() FROM ecommerce_ods.inventory_movements_sink FINAL",
        parse_int,
    ),
    Check(
        "deleted_orders",
        "SELECT COUNT(*) FROM orders WHERE deleted_at IS NOT NULL",
        "SELECT count() FROM ecommerce_ods.orders_sink FINAL "
        "WHERE deleted_at IS NOT NULL",
        parse_int,
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


def run_checks() -> bool:
    clickhouse = ClickHouseHttpClient()
    all_matched = True
    postgres = get_conn()

    try:
        for check in CHECKS:
            pg_value = postgres_scalar(postgres, check.postgres_sql)
            ch_value = clickhouse.scalar(check.clickhouse_sql, check.parser)

            if pg_value == ch_value:
                print(f"[PASS] {check.name} matched: {display_value(pg_value)}")
            else:
                all_matched = False
                print(
                    f"[FAIL] {check.name} mismatch: "
                    f"postgres={display_value(pg_value)} "
                    f"clickhouse={display_value(ch_value)}"
                )
    finally:
        postgres.close()

    return all_matched


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
