"""Streaming throughput stress test for CDC pipeline.

Generates configurable INSERT/UPDATE/DELETE events per minute into PostgreSQL
to test CDC pipeline saturation point.

Usage:
    python -m scripts.benchmark.stress_stream --rate 1000 --duration 600
    python -m scripts.benchmark.stress_stream --rate 5000 --duration 600 --warmup 60
    make stress-stream RATE=1000 DURATION=600

Cumulative design: run this script multiple times at increasing rates.
Each run adds data; there is no automatic reset between runs.
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

from scripts.database.connection import get_conn

# ---------------------------------------------------------------------------
# Distribution of operation types
# ---------------------------------------------------------------------------
OP_INSERT = "INSERT"
OP_UPDATE = "UPDATE"
OP_DELETE = "DELETE"

# 60% insert, 30% update, 10% soft delete
OP_WEIGHTS = [60, 30, 10]
OPS = [OP_INSERT, OP_UPDATE, OP_DELETE]

ORDER_STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_STATUSES = ["PENDING", "PAID", "FAILED", "REFUNDED"]
SHIPMENT_STATUSES = ["PENDING", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "FAILED"]


# ---------------------------------------------------------------------------
# Token bucket rate limiter
# ---------------------------------------------------------------------------
class TokenBucket:
    """Simple token bucket for rate limiting ops/second."""

    def __init__(self, rate_per_second: float) -> None:
        self._rate = rate_per_second
        self._tokens = rate_per_second
        self._last_check = time.monotonic()

    def consume(self, tokens: int = 1) -> None:
        """Block until enough tokens are available, then consume them."""
        while True:
            now = time.monotonic()
            elapsed = now - self._last_check
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_check = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return

            # Sleep a fraction of what we need to wait
            deficit = tokens - self._tokens
            wait = deficit / self._rate
            time.sleep(min(wait, 0.05))


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def insert_order(cur: psycopg2.extensions.cursor, run_id: str, seq: int) -> None:
    cur.execute(
        """
        INSERT INTO orders (
            customer_id, order_status, order_date,
            total_amount, discount_amount, shipping_fee, final_amount,
            payment_status, shipping_address, shipping_city, shipping_country
        )
        SELECT
            c.customer_id,
            'PENDING',
            NOW(),
            (10000 + (%(seq)s %% 99000))::NUMERIC(18,2),
            0,
            15000,
            (10000 + (%(seq)s %% 99000) + 15000)::NUMERIC(18,2),
            'PENDING',
            'stress:' || %(run_id)s || ':' || %(seq)s,
            'Stress City ' || (%(seq)s %% 64),
            'Vietnam'
        FROM customers c
        WHERE c.deleted_at IS NULL
          AND c.customer_id >= GREATEST(
              1,
              FLOOR(RANDOM() * (SELECT COALESCE(MAX(customer_id), 1) FROM customers))::BIGINT
          )
        ORDER BY c.customer_id
        LIMIT 1
        """,
        {"run_id": run_id, "seq": seq},
    )


def insert_payment(cur: psycopg2.extensions.cursor, run_id: str, seq: int) -> None:
    cur.execute(
        """
        INSERT INTO payments (
            order_id, payment_method, payment_status, payment_amount, transaction_code, paid_at
        )
        SELECT
            o.order_id,
            (ARRAY['COD', 'CREDIT_CARD', 'BANK_TRANSFER', 'MOMO'])[1 + (%(seq)s %% 4)],
            'PAID',
            o.final_amount,
            'stress-' || %(run_id)s || '-' || %(seq)s,
            NOW()
        FROM orders o
        WHERE o.payment_status = 'PENDING'
          AND o.deleted_at IS NULL
          AND o.order_id >= GREATEST(
              1,
              FLOOR(RANDOM() * (SELECT COALESCE(MAX(order_id), 1) FROM orders))::BIGINT
          )
        ORDER BY o.order_id
        LIMIT 1
        ON CONFLICT DO NOTHING
        """,
        {"run_id": run_id, "seq": seq},
    )


def update_order_status(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(
        """
        WITH target AS (
            SELECT order_id
            FROM orders
            WHERE deleted_at IS NULL
              AND order_id >= GREATEST(
                  1,
                  FLOOR(RANDOM() * (SELECT COALESCE(MAX(order_id), 1) FROM orders))::BIGINT
              )
            ORDER BY order_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE orders o
        SET order_status = (
                ARRAY['CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED']
            )[1 + MOD(EXTRACT(EPOCH FROM NOW())::BIGINT, 4)],
            updated_at = NOW()
        FROM target
        WHERE o.order_id = target.order_id
        """
    )


def update_inventory(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(
        """
        WITH target AS (
            SELECT product_id
            FROM products
            WHERE deleted_at IS NULL
              AND product_id >= GREATEST(
                  1,
                  FLOOR(RANDOM() * (SELECT COALESCE(MAX(product_id), 1) FROM products))::BIGINT
              )
            ORDER BY product_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE products p
        SET stock_quantity = GREATEST(0, stock_quantity + (FLOOR(RANDOM() * 20 - 5))::INT),
            updated_at = NOW()
        FROM target
        WHERE p.product_id = target.product_id
        """
    )


def soft_delete_old_order(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(
        """
        WITH target AS (
            SELECT order_id
            FROM orders
            WHERE deleted_at IS NULL
              AND order_status = 'CANCELLED'
              AND order_id >= GREATEST(
                  1,
                  FLOOR(RANDOM() * (SELECT COALESCE(MAX(order_id), 1) FROM orders))::BIGINT
              )
            ORDER BY order_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE orders o
        SET deleted_at = NOW(),
            updated_at = NOW()
        FROM target
        WHERE o.order_id = target.order_id
        """
    )


def do_one_event_without_commit(cur, run_id: str, seq: int, op: str) -> None:
    """Execute one CDC event; caller owns transaction commit/rollback."""
    if op == OP_INSERT:
        insert_order(cur, run_id, seq)
        # 50% chance also insert a matching payment
        if seq % 2 == 0:
            insert_payment(cur, run_id, seq)
    elif op == OP_UPDATE:
        # Alternate between order status update and inventory update
        if seq % 3 == 0:
            update_inventory(cur)
        else:
            update_order_status(cur)
    elif op == OP_DELETE:
        soft_delete_old_order(cur)


def do_one_event(conn, cur, run_id: str, seq: int, op: str) -> None:
    """Execute one CDC event and commit."""
    do_one_event_without_commit(cur, run_id, seq, op)
    conn.commit()


# ---------------------------------------------------------------------------
# Metrics collection helpers
# ---------------------------------------------------------------------------
def get_slot_lag(conn) -> int:
    """Return max slot lag in bytes across all logical slots."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
            ), 0)::BIGINT
            FROM pg_replication_slots
            WHERE slot_type = 'logical'
            """
        )
        return int(cur.fetchone()[0])


def format_bytes(value: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value} B"


# ---------------------------------------------------------------------------
# Main stress loop
# ---------------------------------------------------------------------------
def run_stress(
    rate_per_min: int,
    duration_s: int,
    warmup_s: int,
    run_id: str,
    output_path: Path | None,
) -> dict:
    rate_per_sec = rate_per_min / 60.0
    bucket = TokenBucket(rate_per_sec)

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    print(f"🚀 Stress stream started: rate={rate_per_min}/min ({rate_per_sec:.2f}/s)")
    print(f"   duration={duration_s}s  warmup={warmup_s}s  run_id={run_id}")

    start = time.monotonic()
    warmup_end = start + warmup_s
    test_end = start + duration_s

    seq = 0
    total_events = 0
    errors = 0
    max_slot_lag = 0
    slot_lag_samples: list[int] = []

    # Snapshot slot lag every 10s
    last_lag_check = start

    while time.monotonic() < test_end:
        op = random.choices(OPS, weights=OP_WEIGHTS, k=1)[0]
        bucket.consume(1)

        try:
            do_one_event(conn, cur, run_id, seq, op)
            seq += 1

            now = time.monotonic()
            if now >= warmup_end:
                total_events += 1

            # Sample slot lag every 10s
            if now - last_lag_check >= 10:
                try:
                    with get_conn() as lag_conn:
                        lag = get_slot_lag(lag_conn)
                    max_slot_lag = max(max_slot_lag, lag)
                    slot_lag_samples.append(lag)
                    elapsed = now - start
                    in_warmup = " [WARMUP]" if now < warmup_end else ""
                    print(
                        f"  t={elapsed:.0f}s  events={seq}  "
                        f"lag={format_bytes(lag)}{in_warmup}",
                        flush=True,
                    )
                except Exception:
                    pass
                last_lag_check = now

        except Exception as exc:
            errors += 1
            conn.rollback()
            print(f"  [ERROR] {exc}", flush=True)
            time.sleep(1)

    # Final lag check
    try:
        with get_conn() as lag_conn:
            final_lag = get_slot_lag(lag_conn)
    except Exception:
        final_lag = -1

    cur.close()
    conn.close()

    elapsed = time.monotonic() - start
    measured_duration = elapsed - warmup_s

    result = {
        "run_id": run_id,
        "rate_per_min": rate_per_min,
        "duration_s": duration_s,
        "warmup_s": warmup_s,
        "total_events_all": seq,
        "measured_events": total_events,
        "errors": errors,
        "measured_duration_s": max(1, measured_duration),
        "actual_rate_per_min": (total_events / max(1, measured_duration)) * 60,
        "max_slot_lag_bytes": max_slot_lag,
        "final_slot_lag_bytes": final_lag,
        "slot_lag_samples": slot_lag_samples,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    print(
        f"\n✅ Stress stream done:"
        f" rate={result['actual_rate_per_min']:.0f}/min"
        f" events={total_events}"
        f" errors={errors}"
        f" max_lag={format_bytes(max_slot_lag)}"
        f" final_lag={format_bytes(final_lag) if final_lag >= 0 else 'n/a'}"
    )

    if output_path:
        _write_report(output_path, result)

    return result


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------
def _write_report(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rate = result["rate_per_min"]
    run_id = result["run_id"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    content = f"""# Stress Stream Result - {rate} events/min

**Run ID:** {run_id}  
**Started at:** {result["started_at"]}  
**Rate target:** {rate} events/min  
**Duration:** {result["duration_s"]}s (warmup {result["warmup_s"]}s excluded from measurement)  
**Actual rate:** {result["actual_rate_per_min"]:.1f} events/min  
**Total events (including warmup):** {result["total_events_all"]}  
**Measured events (post-warmup):** {result["measured_events"]}  
**Errors:** {result["errors"]}  

## Slot Lag

| Metric | Value |
| --- | --- |
| Max slot lag during test | {format_bytes(result["max_slot_lag_bytes"])} |
| Final slot lag (at end of stream) | {format_bytes(result["final_slot_lag_bytes"]) if result["final_slot_lag_bytes"] >= 0 else "n/a"} |

## Metric Placeholders (fill after test)

| Metric | Value |
| --- | --- |
| p50 latency | |
| p95 latency | |
| p99 latency | |
| max latency | |
| Checkpoint duration avg | |
| Checkpoint failed count | |
| Flink backpressure max | |
| flink-taskmanager CPU peak | |
| flink-taskmanager RAM peak | |
| Validation result | |
| Conclusion | Normal / Saturation / Breaking |

## Notes

_Fill in observations, Grafana screenshots, and conclusions here._
"""
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    with path.open("a", encoding="utf-8") as f:
        if not existing:
            f.write(content)
        else:
            f.write(f"\n\n---\n\n## Re-run {run_id} at {now}\n\n")
            f.write(
                f"- Rate target: {rate}/min\n"
                f"- Actual rate: {result['actual_rate_per_min']:.1f}/min\n"
                f"- Measured events: {result['measured_events']}\n"
                f"- Max slot lag: {format_bytes(result['max_slot_lag_bytes'])}\n"
                f"- Errors: {result['errors']}\n"
            )

    print(f"📝 Report written to: {path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CDC streaming throughput stress test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=1000,
        help="Target events per minute (INSERT+UPDATE+DELETE combined)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=600,
        help="Total test duration in seconds (including warmup)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=60,
        help="Warmup period in seconds (events generated but not counted in metrics)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Run ID for this test (default: timestamp)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to write markdown result file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.duration <= args.warmup:
        print(
            f"ERROR: --duration ({args.duration}s) must be greater than --warmup ({args.warmup}s)",
            file=sys.stderr,
        )
        return 1

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    output_path = args.output
    if output_path is None:
        output_path = Path(
            f"docs/Version 4/RESULT_stress_{args.rate}epm_{run_id}.md"
        )

    run_stress(
        rate_per_min=args.rate,
        duration_s=args.duration,
        warmup_s=args.warmup,
        run_id=run_id,
        output_path=output_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
