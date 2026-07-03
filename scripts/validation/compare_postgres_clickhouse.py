"""Compare key PostgreSQL metrics with their ClickHouse CDC copies.

Run from the project root with:
    python -m scripts.validation.compare_postgres_clickhouse
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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


@dataclass(frozen=True)
class CheckResult:
    check: Check
    postgres_value: object
    clickhouse_value: object
    passed: bool


VALIDATION_HISTORY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cdc_validation_runs (
    run_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    duration_seconds NUMERIC(12, 3) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
    total_checks INTEGER NOT NULL,
    passed_checks INTEGER NOT NULL,
    failed_checks INTEGER NOT NULL,
    attempts INTEGER NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'cli',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cdc_validation_check_results (
    result_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES cdc_validation_runs(run_id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    postgres_value TEXT,
    clickhouse_value TEXT,
    passed BOOLEAN NOT NULL,
    mismatch_detail TEXT,
    attempt INTEGER NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cdc_validation_runs_started_at
    ON cdc_validation_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_cdc_validation_runs_status
    ON cdc_validation_runs(status);
CREATE INDEX IF NOT EXISTS idx_cdc_validation_check_results_run_id
    ON cdc_validation_check_results(run_id);
CREATE INDEX IF NOT EXISTS idx_cdc_validation_check_results_check_name
    ON cdc_validation_check_results(check_name);
CREATE INDEX IF NOT EXISTS idx_cdc_validation_check_results_passed
    ON cdc_validation_check_results(passed);
"""


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
        self.timeout = float(os.getenv("VALIDATION_TIMEOUT", "60"))

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


def configure_postgres_validation_session(conn) -> None:
    # Large local Docker runs can exhaust the default /dev/shm when PostgreSQL
    # chooses parallel plans for validation aggregates.
    with conn.cursor() as cursor:
        cursor.execute("SET max_parallel_workers_per_gather = 0")


def display_value(value: object) -> str:
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    return str(value)


def run_check_results_once() -> list[CheckResult]:
    clickhouse = ClickHouseHttpClient()
    results = []
    postgres = get_conn()

    try:
        configure_postgres_validation_session(postgres)
        for check in CHECKS:
            pg_value = postgres_scalar(postgres, check.postgres_sql)
            ch_value = clickhouse.scalar(check.clickhouse_sql, check.parser)
            results.append(
                CheckResult(
                    check=check,
                    postgres_value=pg_value,
                    clickhouse_value=ch_value,
                    passed=pg_value == ch_value,
                )
            )
    finally:
        postgres.close()

    return results


def run_checks_once() -> list[tuple[Check, object, object]]:
    results = run_check_results_once()
    mismatches = [
        (result.check, result.postgres_value, result.clickhouse_value)
        for result in results
        if not result.passed
    ]
    return mismatches


def validation_history_enabled() -> bool:
    value = os.getenv("VALIDATION_HISTORY_ENABLED", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def validation_history_source() -> str:
    return os.getenv("VALIDATION_HISTORY_SOURCE", "cli").strip() or "cli"


def exception_detail(exc: Exception) -> str:
    return str(exc).strip() or repr(exc)


def mismatch_detail(result: CheckResult) -> str | None:
    if result.passed:
        return None
    return (
        f"postgres={display_value(result.postgres_value)}; "
        f"clickhouse={display_value(result.clickhouse_value)}"
    )


def write_validation_history(
    *,
    started_at: datetime,
    duration_seconds: float,
    attempts: int,
    results: list[CheckResult],
    status: str,
    error_message: str | None = None,
) -> None:
    if not validation_history_enabled():
        return

    finished_at = datetime.now(timezone.utc)
    passed_checks = sum(1 for result in results if result.passed)
    failed_checks = len(results) - passed_checks
    if status == "ERROR" and not results:
        failed_checks = len(CHECKS)

    postgres = get_conn()
    try:
        with postgres:
            with postgres.cursor() as cursor:
                cursor.execute(VALIDATION_HISTORY_SCHEMA_SQL)
                cursor.execute(
                    """
                    INSERT INTO cdc_validation_runs (
                        started_at,
                        finished_at,
                        duration_seconds,
                        status,
                        total_checks,
                        passed_checks,
                        failed_checks,
                        attempts,
                        source,
                        error_message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING run_id
                    """,
                    (
                        started_at,
                        finished_at,
                        round(duration_seconds, 3),
                        status,
                        len(CHECKS),
                        passed_checks,
                        failed_checks,
                        attempts,
                        validation_history_source(),
                        error_message,
                    ),
                )
                run_id = cursor.fetchone()[0]
                cursor.executemany(
                    """
                    INSERT INTO cdc_validation_check_results (
                        run_id,
                        check_name,
                        postgres_value,
                        clickhouse_value,
                        passed,
                        mismatch_detail,
                        attempt
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            run_id,
                            result.check.name,
                            display_value(result.postgres_value),
                            display_value(result.clickhouse_value),
                            result.passed,
                            mismatch_detail(result),
                            attempts,
                        )
                        for result in results
                    ],
                )
    finally:
        postgres.close()


def record_validation_history_best_effort(**kwargs: object) -> None:
    try:
        write_validation_history(**kwargs)
    except Exception as exc:  # noqa: BLE001 - history must not change validation result.
        print(
            f"[WARN] validation history was not written: {exception_detail(exc)}",
            file=sys.stderr,
        )


def run_checks(started_at: datetime | None = None) -> bool:
    timeout = float(os.getenv("VALIDATION_POLL_TIMEOUT", "30"))
    interval = float(os.getenv("VALIDATION_POLL_INTERVAL", "3"))
    started_at = started_at or datetime.now(timezone.utc)
    run_started = time.monotonic()
    deadline = time.monotonic() + timeout
    attempt = 0
    last_results: list[CheckResult] = []

    while True:
        attempt += 1
        last_results = run_check_results_once()
        last_mismatches = [
            (result.check, result.postgres_value, result.clickhouse_value)
            for result in last_results
            if not result.passed
        ]
        if not last_mismatches:
            break
        if time.monotonic() >= deadline:
            break
        print(
            f"[WAIT] {len(last_mismatches)}/{len(CHECKS)} checks still catching up "
            f"(attempt {attempt}); retrying in {interval:g}s..."
        )
        time.sleep(interval)

    failed_names = {
        result.check.name
        for result in last_results
        if not result.passed
    }
    for result in last_results:
        if result.passed:
            print(f"[PASS] {result.check.name}")
        else:
            print(
                f"[FAIL] {result.check.name}: "
                f"postgres={display_value(result.postgres_value)} "
                f"clickhouse={display_value(result.clickhouse_value)}"
            )

    passed = len(CHECKS) - len(failed_names)
    print(f"\n{passed}/{len(CHECKS)} PASS")
    record_validation_history_best_effort(
        started_at=started_at,
        duration_seconds=time.monotonic() - run_started,
        attempts=attempt,
        results=last_results,
        status="PASS" if not failed_names else "FAIL",
    )
    return not failed_names


def main() -> int:
    started_at = datetime.now(timezone.utc)
    run_started = time.monotonic()
    try:
        matched = run_checks(started_at=started_at)
    except Exception as exc:
        detail = exception_detail(exc)
        record_validation_history_best_effort(
            started_at=started_at,
            duration_seconds=time.monotonic() - run_started,
            attempts=0,
            results=[],
            status="ERROR",
            error_message=detail,
        )
        print(f"[ERROR] validation could not run: {detail}", file=sys.stderr)
        return 2

    if matched:
        print("\nValidation passed: PostgreSQL and ClickHouse are consistent.")
        return 0

    print("\nValidation failed: one or more checks did not match.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
