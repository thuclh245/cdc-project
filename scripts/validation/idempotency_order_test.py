"""Validate latest-state CDC idempotency and per-key ordering for orders.

Run from the project root:
    python -m scripts.validation.idempotency_order_test
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

from scripts.benchmark.cdc_latency_benchmark import ClickHouseClient
from scripts.database.connection import get_conn
from scripts.validation.compare_postgres_clickhouse import run_checks


JOB_NAME = "ecommerce-postgres-to-clickhouse-cdc"
FINAL_STATUS = "COMPLETED"


@dataclass
class ClickHouseOrderRow:
    order_id: int
    order_status: str
    updated_at: str


@dataclass
class LatestStateOrderValidationResult:
    status: str
    started_at: str
    customer_id: int | None = None
    order_id: int | None = None
    status_sequence: list[str] = field(default_factory=list)
    physical_row_count: int = 0
    final_row_count: int = 0
    final_status: str = ""
    postgres_updated_at: str = ""
    clickhouse_updated_at: str = ""
    timeline: list[str] = field(default_factory=list)
    taskmanager_restart_status: str = "SKIPPED"
    taskmanager_restart_seconds: float = 0.0
    validation_status: str = "NOT_RUN"
    error: str = ""


class LatestStateOrderValidationFailure(Exception):
    def __init__(self, result: LatestStateOrderValidationResult, exit_code: int) -> None:
        super().__init__(result.error)
        self.result = result
        self.exit_code = exit_code


def docker(*args: str, timeout: int = 120) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=False,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    if result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed ({result.returncode}): {output}")
    return output


def flink_jobs() -> list[dict]:
    with urlopen("http://localhost:8081/jobs/overview", timeout=5) as response:
        return json.load(response).get("jobs", [])


def wait_for_flink_running(timeout: float) -> str:
    deadline = time.monotonic() + timeout
    last_state = "MISSING"
    while time.monotonic() < deadline:
        try:
            jobs = flink_jobs()
        except Exception as exc:  # noqa: BLE001 - Flink may be transient during restart.
            last_state = str(exc) or exc.__class__.__name__
            time.sleep(2)
            continue
        for job in jobs:
            if job.get("name") == JOB_NAME:
                last_state = str(job.get("state", "UNKNOWN"))
                if last_state == "RUNNING":
                    return str(job.get("jid") or "")
        time.sleep(2)
    raise RuntimeError(f"Flink CDC job did not become RUNNING: last_state={last_state}")


def create_customer_and_order() -> tuple[int, int, str]:
    unique = f"latest-state-order-{socket.gethostname()}-{int(time.time())}-{time.time_ns()}"
    email = f"{unique}@example.test"

    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO customers (
                    full_name, email, phone, gender, address, city, country, customer_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                RETURNING customer_id
                """,
                (
                    "Latest-state Idempotency Customer",
                    email,
                    "000-000-0000",
                    "N/A",
                    "Latest-state CDC test address",
                    "Ho Chi Minh City",
                    "Vietnam",
                ),
            )
            customer_id = int(cursor.fetchone()[0])
        conn.commit()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO orders (
                    customer_id,
                    order_status,
                    total_amount,
                    discount_amount,
                    shipping_fee,
                    final_amount,
                    payment_status,
                    shipping_address,
                    shipping_city,
                    shipping_country
                )
                VALUES (%s, 'PENDING', 100000, 0, 15000, 115000, 'UNPAID', %s, %s, %s)
                RETURNING order_id
                """,
                (
                    customer_id,
                    "Latest-state CDC test address",
                    "Ho Chi Minh City",
                    "Vietnam",
                ),
            )
            order_id = int(cursor.fetchone()[0])
        conn.commit()
        return customer_id, order_id, email
    finally:
        conn.close()


def update_order_status(order_id: int, status: str) -> None:
    payment_status = "PAID" if status in {"PAID", "SHIPPED", "COMPLETED"} else "UNPAID"
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE orders
                SET order_status = %s,
                    payment_status = %s
                WHERE order_id = %s
                """,
                (status, payment_status, order_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"expected to update one order, updated {cursor.rowcount}")
        conn.commit()
    finally:
        conn.close()
    time.sleep(0.05)


def postgres_order(order_id: int) -> tuple[str, str]:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_status, to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS.US')
                FROM orders
                WHERE order_id = %s
                """,
                (order_id,),
            )
            row = cursor.fetchone()
    finally:
        conn.close()
    if row is None:
        raise RuntimeError(f"PostgreSQL order_id={order_id} not found")
    return str(row[0]), str(row[1])


def parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value.split("+", 1)[0], "%Y-%m-%d %H:%M:%S.%f")


def clickhouse_final_row(clickhouse: ClickHouseClient, order_id: int) -> ClickHouseOrderRow | None:
    value = clickhouse.query(
        f"""
        SELECT order_id, order_status, toString(updated_at)
        FROM ecommerce_ods.orders_sink FINAL
        WHERE order_id = {order_id}
        """
    )
    if not value:
        return None
    parts = value.split("\t")
    if len(parts) != 3:
        raise RuntimeError(f"unexpected ClickHouse final row: {value!r}")
    return ClickHouseOrderRow(int(parts[0]), parts[1], parts[2])


def clickhouse_scalar_int(clickhouse: ClickHouseClient, sql: str) -> int:
    return int(clickhouse.query(sql) or "0")


def wait_for_final_status(
    clickhouse: ClickHouseClient,
    order_id: int,
    expected_status: str,
    timeout: float,
    poll_interval: float,
) -> ClickHouseOrderRow:
    deadline = time.monotonic() + timeout
    last_row: ClickHouseOrderRow | None = None
    while time.monotonic() < deadline:
        last_row = clickhouse_final_row(clickhouse, order_id)
        if last_row and last_row.order_status == expected_status:
            return last_row
        time.sleep(poll_interval)
    last_status = last_row.order_status if last_row else "MISSING"
    raise AssertionError(
        f"ClickHouse FINAL did not reach {expected_status} within {timeout:g}s "
        f"(last_status={last_status})"
    )


def clickhouse_timeline(clickhouse: ClickHouseClient, order_id: int) -> list[str]:
    value = clickhouse.query(
        f"""
        SELECT concat(order_status, ' @ ', toString(updated_at))
        FROM ecommerce_ods.orders_sink
        WHERE order_id = {order_id}
        ORDER BY updated_at, order_status
        """
    )
    return [line for line in value.splitlines() if line]


def restart_taskmanager(timeout: float) -> tuple[str, float]:
    started = time.monotonic()
    docker("restart", "flink-taskmanager", timeout=120)
    wait_for_flink_running(timeout)
    return "PASS", time.monotonic() - started


def run_latest_state_order_validation(
    args: argparse.Namespace,
) -> LatestStateOrderValidationResult:
    result = LatestStateOrderValidationResult(
        status="FAIL",
        started_at=datetime.now().isoformat(timespec="seconds"),
    )
    clickhouse = ClickHouseClient()

    try:
        wait_for_flink_running(args.wait_timeout)
        customer_id, order_id, _email = create_customer_and_order()
        result.customer_id = customer_id
        result.order_id = order_id
        result.status_sequence.append("PENDING")
        print(f"[OK] created customer_id={customer_id}, order_id={order_id}")

        wait_for_final_status(clickhouse, order_id, "PENDING", args.wait_timeout, args.poll_interval)
        print("[OK] initial PENDING order reached ClickHouse FINAL")

        for status in ("PAID", "SHIPPED"):
            update_order_status(order_id, status)
            result.status_sequence.append(status)
            print(f"[OK] updated PostgreSQL order to {status}")

        if not args.skip_taskmanager_restart:
            print("[INFO] restarting flink-taskmanager between updates...")
            restart_status, restart_seconds = restart_taskmanager(args.wait_timeout)
            result.taskmanager_restart_status = restart_status
            result.taskmanager_restart_seconds = restart_seconds
            print(f"[OK] TaskManager restart {restart_status} in {restart_seconds:.1f}s")

        update_order_status(order_id, FINAL_STATUS)
        result.status_sequence.append(FINAL_STATUS)
        print(f"[OK] updated PostgreSQL order to {FINAL_STATUS}")

        final_row = wait_for_final_status(
            clickhouse,
            order_id,
            FINAL_STATUS,
            args.wait_timeout,
            args.poll_interval,
        )
        pg_status, pg_updated_at = postgres_order(order_id)
        result.postgres_updated_at = pg_updated_at
        result.clickhouse_updated_at = final_row.updated_at
        result.final_status = final_row.order_status
        result.physical_row_count = clickhouse_scalar_int(
            clickhouse,
            f"SELECT count() FROM ecommerce_ods.orders_sink WHERE order_id = {order_id}",
        )
        result.final_row_count = clickhouse_scalar_int(
            clickhouse,
            f"SELECT count() FROM ecommerce_ods.orders_sink FINAL WHERE order_id = {order_id}",
        )
        result.timeline = clickhouse_timeline(clickhouse, order_id)

        if pg_status != FINAL_STATUS:
            raise AssertionError(f"PostgreSQL final status is {pg_status}, expected {FINAL_STATUS}")
        if result.final_status != FINAL_STATUS:
            raise AssertionError(
                f"ClickHouse FINAL status is {result.final_status}, expected {FINAL_STATUS}"
            )
        if result.final_row_count != 1:
            raise AssertionError(
                f"ClickHouse FINAL row count is {result.final_row_count}, expected 1"
            )
        if result.physical_row_count < 1:
            raise AssertionError("ClickHouse physical row count must be at least 1")
        if parse_timestamp(result.clickhouse_updated_at) < parse_timestamp(result.postgres_updated_at):
            raise AssertionError(
                "ClickHouse FINAL updated_at is older than PostgreSQL source "
                f"({result.clickhouse_updated_at} < {result.postgres_updated_at})"
            )

        print("[INFO] running full PostgreSQL-vs-ClickHouse validation...")
        validation_ok = run_checks()
        result.validation_status = "PASS" if validation_ok else "FAIL"
        if not validation_ok:
            raise AssertionError("full validation failed after latest-state idempotency/order test")

        result.status = "PASS"
        return result
    except AssertionError as exc:
        result.status = "FAIL"
        result.error = str(exc) or exc.__class__.__name__
        raise LatestStateOrderValidationFailure(result, 1) from exc
    except Exception as exc:
        result.status = "ERROR"
        result.error = str(exc) or exc.__class__.__name__
        raise LatestStateOrderValidationFailure(result, 2) from exc


def render_report(result: LatestStateOrderValidationResult) -> str:
    timeline = "\n".join(f"- {item}" for item in result.timeline) or "- n/a"
    sequence = " -> ".join(result.status_sequence) or "n/a"
    return f"""# Latest-state idempotency/order validation result

## Summary

- Status: `{result.status}`
- Started at: `{result.started_at}`
- Customer ID: `{result.customer_id or 'n/a'}`
- Order ID: `{result.order_id or 'n/a'}`
- Status sequence: `{sequence}`
- TaskManager restart: `{result.taskmanager_restart_status}` ({result.taskmanager_restart_seconds:.1f}s)
- Full validation: `{result.validation_status}`

## Latest-state checks

| Check | Value |
| --- | --- |
| ClickHouse physical rows without FINAL | `{result.physical_row_count}` |
| ClickHouse logical rows with FINAL | `{result.final_row_count}` |
| ClickHouse FINAL status | `{result.final_status or 'n/a'}` |
| PostgreSQL updated_at | `{result.postgres_updated_at or 'n/a'}` |
| ClickHouse FINAL updated_at | `{result.clickhouse_updated_at or 'n/a'}` |

## ClickHouse physical timeline

{timeline}

## Conclusion

This test validates idempotency/order within the latest-state CDC scope: repeated updates for the same `order_id` and a TaskManager restart still converge to one logical `orders_sink FINAL` row with status `COMPLETED`.

## Error

```text
{result.error or 'n/a'}
```
"""


def write_report(path: Path, result: LatestStateOrderValidationResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_report(result), encoding="utf-8")
    print(f"[OK] wrote report: {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run latest-state idempotency/order test.")
    parser.add_argument("--wait-timeout", type=float, default=180.0)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--skip-taskmanager-restart", action="store_true")
    parser.add_argument("--report-file", type=Path)
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    args = parse_args()
    if args.wait_timeout <= 0:
        print("[ERROR] --wait-timeout must be greater than 0", file=sys.stderr)
        return 2
    if args.poll_interval <= 0:
        print("[ERROR] --poll-interval must be greater than 0", file=sys.stderr)
        return 2

    try:
        result = run_latest_state_order_validation(args)
        exit_code = 0
    except LatestStateOrderValidationFailure as exc:
        result = exc.result
        prefix = "[FAIL]" if exc.exit_code == 1 else "[ERROR]"
        print(f"{prefix} {result.error}", file=sys.stderr)
        exit_code = exc.exit_code

    if args.report_file:
        write_report(args.report_file, result)

    print(f"status={result.status}")
    print(f"order_id={result.order_id or 'n/a'}")
    print(f"physical_rows={result.physical_row_count}")
    print(f"final_rows={result.final_row_count}")
    print(f"final_status={result.final_status or 'n/a'}")
    print(f"validation={result.validation_status}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
