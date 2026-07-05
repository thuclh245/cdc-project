"""Large-data CDC load test helper.

Run from the project root:
    python -m scripts.benchmark.large_data_load --target-size 500MB
    python -m scripts.benchmark.large_data_load --customers 10000 --products 1000 --orders 50000
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from scripts.benchmark.cdc_latency_benchmark import (
    ClickHouseClient,
    insert_probe,
    percentile,
    wait_for_probe,
)
from scripts.common.constants import BRANDS, CATEGORIES
from scripts.database.connection import get_conn
from scripts.seeders.seed_categories import seed_categories


CDC_TABLES = (
    "customers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory_movements",
    "cdc_latency_probe",
)

SINK_TABLES = {table: f"{table}_sink" for table in CDC_TABLES}
APP_TABLES = tuple(table for table in CDC_TABLES if table != "cdc_latency_probe")
DEFAULT_REPORT_DIR = Path("docs/Version 3")
DEFAULT_ORDER_FAMILY_BYTES = 2_500


@dataclass
class LoadConfig:
    run_id: str
    target_size_bytes: int | None
    customers: int
    products: int
    orders: int
    batch_size: int
    product_stock: int
    wait_timeout: float
    latency_samples: int
    latency_interval: float
    latency_timeout: float
    validate: bool
    report_file: Path | None
    max_orders: int | None


@dataclass
class ValidationResult:
    status: str
    duration_seconds: float
    output_tail: str


def parse_size(value: str) -> int:
    units = {
        "B": 1,
        "KB": 1024,
        "K": 1024,
        "MB": 1024**2,
        "M": 1024**2,
        "GB": 1024**3,
        "G": 1024**3,
    }
    normalized = value.strip().upper()
    for suffix, multiplier in sorted(units.items(), key=lambda item: len(item[0]), reverse=True):
        if normalized.endswith(suffix):
            number = normalized[: -len(suffix)].strip()
            return int(Decimal(number) * multiplier)
    return int(Decimal(normalized))


def format_bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{value} B"


def format_seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}s"


def source_table_counts(conn) -> dict[str, int]:
    counts = {}
    with conn.cursor() as cursor:
        for table in CDC_TABLES:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = int(cursor.fetchone()[0])
    return counts


def source_size_bytes(conn) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COALESCE(SUM(pg_total_relation_size(format('public.%%I', tablename))), 0)
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename = ANY(%s)
            """,
            (list(CDC_TABLES),),
        )
        return int(cursor.fetchone()[0])


def max_slot_lag_bytes(conn) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COALESCE(MAX(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)::BIGINT
            FROM pg_replication_slots
            WHERE slot_type = 'logical'
            """
        )
        return int(cursor.fetchone()[0])


def clickhouse_sink_counts(clickhouse: ClickHouseClient, *, final: bool) -> dict[str, int]:
    final_suffix = " FINAL" if final else ""
    sql = "\nUNION ALL\n".join(
        f"SELECT '{table}' AS table_name, count() AS row_count "
        f"FROM ecommerce_ods.{sink}{final_suffix}"
        for table, sink in SINK_TABLES.items()
    )
    rows = clickhouse.query(sql)
    counts: dict[str, int] = {}
    if not rows:
        return counts
    for line in rows.splitlines():
        table, count = line.split("\t", 1)
        counts[table] = int(count)
    return counts


def clickhouse_physical_rows(clickhouse: ClickHouseClient) -> int:
    tables = ", ".join(f"'{sink}'" for sink in SINK_TABLES.values())
    sql = f"""
        SELECT COALESCE(SUM(total_rows), 0)
        FROM system.tables
        WHERE database = 'ecommerce_ods'
          AND name IN ({tables})
    """
    return int(clickhouse.query(sql) or "0")


def ensure_category_rows(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM categories")
        count = int(cursor.fetchone()[0])
    if count < len(CATEGORIES):
        seed_categories(conn)


def insert_customers(conn, total: int, batch_size: int, run_id: str) -> int:
    if total <= 0:
        return 0

    print(f"Loading customers: {total}")
    inserted = 0
    while inserted < total:
        chunk = min(batch_size, total - inserted)
        start = inserted + 1
        end = inserted + chunk
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO customers (
                    full_name,
                    email,
                    phone,
                    gender,
                    date_of_birth,
                    address,
                    city,
                    country,
                    customer_status
                )
                SELECT
                    'Load Customer ' || %(run_id)s || '-' || g,
                    'load_' || %(run_id)s || '_' || g || '@example.com',
                    '+84' || lpad((g %% 1000000000)::TEXT, 9, '0'),
                    (ARRAY['MALE', 'FEMALE', 'OTHER'])[1 + (g %% 3)],
                    DATE '1970-01-01' + ((g %% 16000)::INT),
                    'Load address ' || g,
                    'Load City ' || (g %% 64),
                    'Vietnam',
                    'ACTIVE'
                FROM generate_series(%(start)s, %(end)s) AS g
                ON CONFLICT (email) DO NOTHING
                """,
                {"run_id": run_id, "start": start, "end": end},
            )
        conn.commit()
        inserted += chunk
        print(f"  customers: {inserted}/{total}")

    return inserted


def insert_products(conn, total: int, batch_size: int, run_id: str, product_stock: int) -> int:
    if total <= 0:
        return 0

    print(f"Loading products: {total}")
    categories = list(CATEGORIES)
    brands = list(BRANDS)
    inserted = 0
    while inserted < total:
        chunk = min(batch_size, total - inserted)
        start = inserted + 1
        end = inserted + chunk
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH inserted_products AS (
                    INSERT INTO products (
                        product_name,
                        category,
                        brand,
                        price,
                        cost,
                        stock_quantity,
                        product_status
                    )
                    SELECT
                        'Load Product ' || %(run_id)s || '-' || g,
                        (%(categories)s::TEXT[])[(g %% %(category_count)s) + 1],
                        (%(brands)s::TEXT[])[(g %% %(brand_count)s) + 1],
                        ((1000 + (g %% 200000))::NUMERIC / 10)::NUMERIC(18, 2),
                        ((700 + (g %% 140000))::NUMERIC / 10)::NUMERIC(18, 2),
                        %(product_stock)s,
                        'ACTIVE'
                    FROM generate_series(%(start)s, %(end)s) AS g
                    RETURNING product_id, stock_quantity
                )
                INSERT INTO inventory_movements (
                    product_id,
                    order_id,
                    movement_type,
                    quantity_change,
                    old_stock,
                    new_stock,
                    reason
                )
                SELECT
                    product_id,
                    NULL,
                    'IMPORT',
                    stock_quantity,
                    0,
                    stock_quantity,
                    'Large data load initial stock'
                FROM inserted_products
                """,
                {
                    "run_id": run_id,
                    "categories": categories,
                    "category_count": len(categories),
                    "brands": brands,
                    "brand_count": len(brands),
                    "product_stock": product_stock,
                    "start": start,
                    "end": end,
                },
            )
        conn.commit()
        inserted += chunk
        print(f"  products: {inserted}/{total}")

    return inserted


def rebuild_load_pools(conn) -> tuple[int, int]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS large_load_customer_pool (
                rn BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL
            ) ON COMMIT PRESERVE ROWS
            """
        )
        cursor.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS large_load_product_pool (
                rn BIGINT PRIMARY KEY,
                product_id BIGINT NOT NULL,
                price NUMERIC(18, 2) NOT NULL
            ) ON COMMIT PRESERVE ROWS
            """
        )
        cursor.execute("TRUNCATE large_load_customer_pool")
        cursor.execute("TRUNCATE large_load_product_pool")
        cursor.execute(
            """
            INSERT INTO large_load_customer_pool (rn, customer_id)
            SELECT row_number() OVER (ORDER BY customer_id), customer_id
            FROM customers
            WHERE deleted_at IS NULL
              AND customer_status = 'ACTIVE'
            """
        )
        cursor.execute(
            """
            INSERT INTO large_load_product_pool (rn, product_id, price)
            SELECT row_number() OVER (ORDER BY product_id), product_id, price
            FROM products
            WHERE deleted_at IS NULL
              AND product_status = 'ACTIVE'
            """
        )
        cursor.execute("SELECT COUNT(*) FROM large_load_customer_pool")
        customer_count = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM large_load_product_pool")
        product_count = int(cursor.fetchone()[0])

    conn.commit()

    if customer_count == 0:
        raise RuntimeError("cannot load orders: no active customers")
    if product_count == 0:
        raise RuntimeError("cannot load orders: no active products")

    return customer_count, product_count


def insert_order_batch(
    conn,
    *,
    run_id: str,
    start_seq: int,
    end_seq: int,
    customer_count: int,
    product_count: int,
) -> int:
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS large_load_order_batch")
        cursor.execute(
            """
            CREATE TEMP TABLE large_load_order_batch (
                seq BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL,
                product_id BIGINT NOT NULL,
                quantity INT NOT NULL,
                unit_price NUMERIC(18, 2) NOT NULL,
                item_discount NUMERIC(18, 2) NOT NULL,
                item_total NUMERIC(18, 2) NOT NULL,
                shipping_fee NUMERIC(18, 2) NOT NULL,
                order_discount NUMERIC(18, 2) NOT NULL,
                final_amount NUMERIC(18, 2) NOT NULL,
                shipping_address TEXT NOT NULL,
                shipping_city TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
        cursor.execute(
            """
            INSERT INTO large_load_order_batch (
                seq,
                customer_id,
                product_id,
                quantity,
                unit_price,
                item_discount,
                item_total,
                shipping_fee,
                order_discount,
                final_amount,
                shipping_address,
                shipping_city
            )
            SELECT
                g,
                c.customer_id,
                p.product_id,
                (1 + (g %% 5))::INT AS quantity,
                p.price,
                ((g %% 250)::NUMERIC / 10)::NUMERIC(18, 2) AS item_discount,
                GREATEST(
                    ((p.price * (1 + (g %% 5))) - ((g %% 250)::NUMERIC / 10)),
                    0
                )::NUMERIC(18, 2) AS item_total,
                (10000 + (g %% 50000))::NUMERIC(18, 2) AS shipping_fee,
                ((g %% 150)::NUMERIC / 10)::NUMERIC(18, 2) AS order_discount,
                GREATEST(
                    ((p.price * (1 + (g %% 5))) - ((g %% 250)::NUMERIC / 10))
                    + (10000 + (g %% 50000))
                    - ((g %% 150)::NUMERIC / 10),
                    0
                )::NUMERIC(18, 2) AS final_amount,
                'large-load:' || %(run_id)s || ':' || g AS shipping_address,
                'Load City ' || (g %% 64) AS shipping_city
            FROM generate_series(%(start_seq)s, %(end_seq)s) AS g
            JOIN large_load_customer_pool c
              ON c.rn = ((g - 1) %% %(customer_count)s) + 1
            JOIN large_load_product_pool p
              ON p.rn = ((g - 1) %% %(product_count)s) + 1
            """,
            {
                "run_id": run_id,
                "start_seq": start_seq,
                "end_seq": end_seq,
                "customer_count": customer_count,
                "product_count": product_count,
            },
        )
        cursor.execute(
            """
            INSERT INTO orders (
                customer_id,
                order_status,
                order_date,
                total_amount,
                discount_amount,
                shipping_fee,
                final_amount,
                payment_status,
                shipping_address,
                shipping_city,
                shipping_country
            )
            SELECT
                customer_id,
                CASE WHEN seq % 4 = 0 THEN 'CONFIRMED' ELSE 'PENDING' END,
                CURRENT_TIMESTAMP,
                item_total,
                order_discount,
                shipping_fee,
                final_amount,
                'PAID',
                shipping_address,
                shipping_city,
                'Vietnam'
            FROM large_load_order_batch
            ORDER BY seq
            """
        )
        cursor.execute("DROP TABLE IF EXISTS large_load_inserted_orders")
        cursor.execute(
            """
            CREATE TEMP TABLE large_load_inserted_orders ON COMMIT DROP AS
            SELECT
                o.order_id,
                b.seq,
                b.product_id,
                b.quantity,
                b.unit_price,
                b.item_discount,
                b.item_total,
                b.final_amount,
                b.shipping_address,
                b.shipping_city
            FROM large_load_order_batch b
            JOIN orders o ON o.shipping_address = b.shipping_address
            """
        )
        cursor.execute(
            """
            INSERT INTO order_items (
                order_id,
                product_id,
                quantity,
                unit_price,
                discount_amount,
                total_amount
            )
            SELECT
                order_id,
                product_id,
                quantity,
                unit_price,
                item_discount,
                item_total
            FROM large_load_inserted_orders
            """
        )
        cursor.execute(
            """
            INSERT INTO payments (
                order_id,
                payment_method,
                payment_status,
                payment_amount,
                transaction_code,
                paid_at
            )
            SELECT
                order_id,
                (ARRAY['COD', 'CREDIT_CARD', 'BANK_TRANSFER', 'MOMO', 'ZALOPAY', 'VNPAY'])[1 + (seq %% 6)],
                'SUCCESS',
                final_amount,
                'LL-' || %(run_id)s || '-' || seq,
                CURRENT_TIMESTAMP
            FROM large_load_inserted_orders
            """,
            {"run_id": run_id},
        )
        cursor.execute(
            """
            INSERT INTO shipments (
                order_id,
                carrier,
                tracking_number,
                shipment_status,
                shipping_address,
                shipping_city,
                shipping_country
            )
            SELECT
                order_id,
                (ARRAY['GHN', 'GHTK', 'J&T Express', 'Viettel Post', 'VNPost'])[1 + (seq %% 5)],
                'LL-' || %(run_id)s || '-' || seq,
                CASE
                    WHEN seq %% 5 = 0 THEN 'IN_TRANSIT'
                    WHEN seq %% 3 = 0 THEN 'PICKED_UP'
                    ELSE 'PENDING'
                END,
                shipping_address,
                shipping_city,
                'Vietnam'
            FROM large_load_inserted_orders
            """,
            {"run_id": run_id},
        )
        cursor.execute(
            """
            UPDATE products p
            SET stock_quantity = GREATEST(p.stock_quantity - sold.quantity, 0),
                product_status = CASE
                    WHEN GREATEST(p.stock_quantity - sold.quantity, 0) = 0 THEN 'OUT_OF_STOCK'
                    ELSE 'ACTIVE'
                END
            FROM (
                SELECT product_id, SUM(quantity)::INT AS quantity
                FROM large_load_inserted_orders
                GROUP BY product_id
            ) sold
            WHERE p.product_id = sold.product_id
            """
        )
        cursor.execute(
            """
            INSERT INTO inventory_movements (
                product_id,
                order_id,
                movement_type,
                quantity_change,
                old_stock,
                new_stock,
                reason
            )
            SELECT
                product_id,
                order_id,
                'SALE',
                -quantity,
                NULL,
                NULL,
                'Large data load sale'
            FROM large_load_inserted_orders
            """
        )
        cursor.execute("SELECT COUNT(*) FROM large_load_inserted_orders")
        inserted = int(cursor.fetchone()[0])

    conn.commit()
    return inserted


def insert_orders(conn, total: int, batch_size: int, run_id: str, start_seq: int = 1) -> int:
    if total <= 0:
        return 0

    customer_count, product_count = rebuild_load_pools(conn)
    print(
        "Loading orders: "
        f"{total} using {customer_count} active customers and {product_count} active products"
    )

    inserted = 0
    while inserted < total:
        chunk = min(batch_size, total - inserted)
        batch_start_seq = start_seq + inserted
        batch_end_seq = batch_start_seq + chunk - 1
        inserted += insert_order_batch(
            conn,
            run_id=run_id,
            start_seq=batch_start_seq,
            end_seq=batch_end_seq,
            customer_count=customer_count,
            product_count=product_count,
        )
        print(f"  orders: {inserted}/{total}")

    return inserted


def derive_counts_for_target(target_size_bytes: int) -> tuple[int, int]:
    estimated_orders = max(1, target_size_bytes // DEFAULT_ORDER_FAMILY_BYTES)
    customers = max(1_000, min(200_000, estimated_orders // 5))
    products = max(300, min(50_000, estimated_orders // 50))
    return int(customers), int(products)


def load_until_target(conn, config: LoadConfig) -> int:
    if config.target_size_bytes is None:
        return insert_orders(conn, config.orders, config.batch_size, config.run_id)

    current_size = source_size_bytes(conn)
    if current_size >= config.target_size_bytes:
        print(
            "Target size already reached: "
            f"{format_bytes(current_size)} >= {format_bytes(config.target_size_bytes)}"
        )
        return 0

    inserted_orders = 0
    while current_size < config.target_size_bytes:
        if config.max_orders is not None and inserted_orders >= config.max_orders:
            print(f"Reached --max-orders={config.max_orders}; stopping target-size loop.")
            break
        chunk = config.orders if config.orders > 0 else config.batch_size
        if config.max_orders is not None:
            chunk = min(chunk, config.max_orders - inserted_orders)
        if chunk <= 0:
            break

        inserted_orders += insert_orders(
            conn,
            chunk,
            config.batch_size,
            config.run_id,
            start_seq=inserted_orders + 1,
        )
        current_size = source_size_bytes(conn)
        print(
            "  source size: "
            f"{format_bytes(current_size)}/{format_bytes(config.target_size_bytes)} "
            f"(orders inserted in target loop: {inserted_orders})"
        )

    return inserted_orders


def wait_for_sink_catchup(
    conn,
    clickhouse: ClickHouseClient,
    timeout: float,
) -> tuple[float | None, dict[str, int]]:
    if timeout <= 0:
        return None, {}

    deadline = time.monotonic() + timeout
    started = time.monotonic()
    source_counts = source_table_counts(conn)
    last_counts: dict[str, int] = {}

    while time.monotonic() < deadline:
        last_counts = clickhouse_sink_counts(clickhouse, final=True)
        mismatched = [
            table
            for table in APP_TABLES
            if source_counts.get(table, 0) != last_counts.get(table, -1)
        ]
        if not mismatched:
            return time.monotonic() - started, last_counts
        preview = ", ".join(
            f"{table}=pg:{source_counts.get(table, 0)}/ch:{last_counts.get(table, 0)}"
            for table in mismatched[:4]
        )
        print(f"[WAIT] sink catching up: {preview}")
        time.sleep(5)

    return None, last_counts


def run_latency_probe(config: LoadConfig) -> dict[str, float | int]:
    if config.latency_samples <= 0:
        return {}

    clickhouse = ClickHouseClient()
    latencies: list[float] = []
    timeouts = 0
    probe_key = f"large-data-load-{config.run_id}-{socket.gethostname()}"

    print(
        "Running latency probe: "
        f"samples={config.latency_samples} interval={config.latency_interval:g}s"
    )
    for index in range(1, config.latency_samples + 1):
        payload = f"{probe_key}:{index}:{time.time_ns()}"
        probe_id = insert_probe(probe_key, payload)
        committed_at = time.monotonic()
        if wait_for_probe(clickhouse, probe_id, config.latency_timeout):
            latency = time.monotonic() - committed_at
            latencies.append(latency)
            print(f"  latency {index}/{config.latency_samples}: {latency:.3f}s")
        else:
            timeouts += 1
            print(f"  latency {index}/{config.latency_samples}: TIMEOUT")

        if index < config.latency_samples and config.latency_interval > 0:
            time.sleep(config.latency_interval)

    return {
        "samples": config.latency_samples,
        "observed": len(latencies),
        "timeouts": timeouts,
        "mean": (sum(latencies) / len(latencies)) if latencies else 0.0,
        "p50": percentile(latencies, 50),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
        "max": max(latencies) if latencies else 0.0,
    }


def run_validation() -> ValidationResult:
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-m", "scripts.validation.compare_postgres_clickhouse"],
        text=True,
        capture_output=True,
        check=False,
    )
    duration = time.monotonic() - started
    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    tail = "\n".join(output.strip().splitlines()[-16:])
    status = "PASS" if result.returncode == 0 else f"FAIL({result.returncode})"
    print(tail)
    return ValidationResult(status=status, duration_seconds=duration, output_tail=tail)


def format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{table}={counts.get(table, 0)}" for table in APP_TABLES)


def append_report(
    path: Path,
    *,
    config: LoadConfig,
    started_at: datetime,
    load_duration: float,
    catchup_duration: float | None,
    source_size: int,
    source_counts: dict[str, int],
    sink_counts: dict[str, int],
    physical_sink_rows: int,
    max_slot_lag: int,
    validation: ValidationResult | None,
    latency: dict[str, float | int],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    validation_status = validation.status if validation else "SKIP"
    validation_duration = validation.duration_seconds if validation else None
    mean = latency.get("mean")
    p95 = latency.get("p95")
    p99 = latency.get("p99")
    row = (
        f"| {format_bytes(source_size)} "
        f"| {sum(source_counts.get(table, 0) for table in APP_TABLES)} "
        f"| {sum(sink_counts.get(table, 0) for table in APP_TABLES)} "
        f"| {format_seconds(catchup_duration)} "
        f"| {format_seconds(float(mean)) if mean is not None else 'n/a'} "
        f"| {format_seconds(float(p95)) if p95 is not None else 'n/a'} "
        f"| {format_seconds(float(p99)) if p99 is not None else 'n/a'} "
        f"| {format_bytes(max_slot_lag)} "
        f"| {format_seconds(validation_duration)} "
        f"| {validation_status} |\n"
    )
    section = f"""

## Run {config.run_id}

**Started at:** {started_at.isoformat(timespec="seconds")}
**Load duration:** {format_seconds(load_duration)}
**Source size:** {format_bytes(source_size)}
**ClickHouse physical rows:** {physical_sink_rows}
**Inserted request:** customers={config.customers}, products={config.products}, orders={config.orders}, target={format_bytes(config.target_size_bytes)}

| Data size | Source rows | Sink rows FINAL | Sink catch-up | Mean latency | p95 latency | p99 latency | Max slot lag | Validation duration | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
{row}
**Source counts:** {format_counts(source_counts)}

**Sink counts FINAL:** {format_counts(sink_counts)}

"""
    if validation:
        section += f"""**Validation tail:**

```text
{validation.output_tail}
```

"""
    with path.open("a", encoding="utf-8") as file:
        file.write(section)
    print(f"Report appended: {path}")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be greater than or equal to 0")
    return parsed


def build_config(args: argparse.Namespace) -> LoadConfig:
    target_size_bytes = parse_size(args.target_size) if args.target_size else None
    customers = args.customers
    products = args.products
    orders = args.orders

    if target_size_bytes is not None and args.customers is None and args.products is None:
        customers, products = derive_counts_for_target(target_size_bytes)
    if target_size_bytes is not None and args.orders is None:
        orders = args.batch_size

    report_file: Path | None
    if args.no_report:
        report_file = None
    elif args.report_file:
        report_file = Path(args.report_file)
    else:
        today = datetime.now().strftime("%Y-%m-%d")
        report_file = DEFAULT_REPORT_DIR / f"09_LARGE_DATA_LOAD_RESULT_{today}.md"

    return LoadConfig(
        run_id=args.run_id or datetime.now().strftime("%Y%m%d%H%M%S"),
        target_size_bytes=target_size_bytes,
        customers=customers or 0,
        products=products or 0,
        orders=orders or 0,
        batch_size=args.batch_size,
        product_stock=args.product_stock,
        wait_timeout=args.wait_timeout,
        latency_samples=args.latency_samples,
        latency_interval=args.latency_interval,
        latency_timeout=args.latency_timeout,
        validate=not args.skip_validation,
        report_file=report_file,
        max_orders=args.max_orders,
    )


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a large-data CDC load test.")
    parser.add_argument("--target-size", help="target PostgreSQL source data size, e.g. 500MB or 1GB")
    parser.add_argument("--customers", type=positive_int, help="customers to insert before orders")
    parser.add_argument("--products", type=positive_int, help="products to insert before orders")
    parser.add_argument("--orders", type=positive_int, help="orders to insert; with --target-size this is the loop chunk")
    parser.add_argument("--batch-size", type=positive_int, default=10_000)
    parser.add_argument("--product-stock", type=positive_int, default=1_000_000)
    parser.add_argument("--max-orders", type=positive_int, help="safety cap for --target-size order loop")
    parser.add_argument("--wait-timeout", type=float, default=300.0, help="seconds to wait for ClickHouse FINAL row counts")
    parser.add_argument("--latency-samples", type=positive_int, default=20)
    parser.add_argument("--latency-interval", type=float, default=1.0)
    parser.add_argument("--latency-timeout", type=float, default=30.0)
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--report-file", help="markdown file to append results")
    parser.add_argument("--no-report", action="store_true")
    parser.add_argument("--run-id", help="stable ID used in generated rows and report")
    args = parser.parse_args(argv)

    if not args.target_size and args.customers is None and args.products is None and args.orders is None:
        parser.error("provide --target-size or at least one of --customers/--products/--orders")
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    if args.wait_timeout < 0:
        parser.error("--wait-timeout must be greater than or equal to 0")
    if args.latency_interval < 0:
        parser.error("--latency-interval must be greater than or equal to 0")
    if args.latency_timeout <= 0:
        parser.error("--latency-timeout must be greater than 0")
    return args


def main(argv: Iterable[str] | None = None) -> int:
    config = build_config(parse_args(argv))
    started_at = datetime.now()
    started = time.monotonic()

    print(
        "Large data load test "
        f"run_id={config.run_id} target={format_bytes(config.target_size_bytes)} "
        f"customers={config.customers} products={config.products} orders={config.orders}"
    )

    conn = get_conn()
    try:
        ensure_category_rows(conn)
        if (
            config.target_size_bytes is not None
            and source_size_bytes(conn) >= config.target_size_bytes
        ):
            print("Target size already reached before dimension load; skipping customers/products.")
        else:
            insert_customers(conn, config.customers, config.batch_size, config.run_id)
            insert_products(conn, config.products, config.batch_size, config.run_id, config.product_stock)
        inserted_orders = load_until_target(conn, config)
        load_duration = time.monotonic() - started
        print(f"Load finished in {format_seconds(load_duration)}; inserted_orders={inserted_orders}")

        clickhouse = ClickHouseClient()
        catchup_duration, sink_counts = wait_for_sink_catchup(conn, clickhouse, config.wait_timeout)
        if catchup_duration is None and config.wait_timeout > 0:
            print("[WARN] sink counts did not catch up before wait timeout")

        latency = run_latency_probe(config)
        validation = run_validation() if config.validate else None

        source_counts = source_table_counts(conn)
        source_size = source_size_bytes(conn)
        if not sink_counts:
            sink_counts = clickhouse_sink_counts(clickhouse, final=True)
        physical_sink_rows = clickhouse_physical_rows(clickhouse)
        slot_lag = max_slot_lag_bytes(conn)

        print()
        print("| Metric | Value |")
        print("| --- | ---: |")
        print(f"| Source size | {format_bytes(source_size)} |")
        print(f"| Source rows | {sum(source_counts.get(table, 0) for table in APP_TABLES)} |")
        print(f"| Sink rows FINAL | {sum(sink_counts.get(table, 0) for table in APP_TABLES)} |")
        print(f"| ClickHouse physical rows | {physical_sink_rows} |")
        print(f"| Load duration | {format_seconds(load_duration)} |")
        print(f"| Sink catch-up duration | {format_seconds(catchup_duration)} |")
        print(f"| Max slot lag | {format_bytes(slot_lag)} |")
        if latency:
            print(f"| Latency mean | {format_seconds(float(latency['mean']))} |")
            print(f"| Latency p95 | {format_seconds(float(latency['p95']))} |")
            print(f"| Latency p99 | {format_seconds(float(latency['p99']))} |")
        print(f"| Validation | {validation.status if validation else 'SKIP'} |")
        if validation:
            print(f"| Validation duration | {format_seconds(validation.duration_seconds)} |")

        if config.report_file:
            append_report(
                config.report_file,
                config=config,
                started_at=started_at,
                load_duration=load_duration,
                catchup_duration=catchup_duration,
                source_size=source_size,
                source_counts=source_counts,
                sink_counts=sink_counts,
                physical_sink_rows=physical_sink_rows,
                max_slot_lag=slot_lag,
                validation=validation,
                latency=latency,
            )

        if validation and validation.status != "PASS":
            return 1
        if catchup_duration is None and config.wait_timeout > 0:
            return 1
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI should print concise operational failures.
        print(f"[ERROR] large data load failed: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
