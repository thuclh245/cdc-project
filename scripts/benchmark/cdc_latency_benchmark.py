"""Measure PostgreSQL-to-ClickHouse CDC latency with a probe table.

Run from the project root:
    python -m scripts.benchmark.cdc_latency_benchmark --samples 100 --interval 1
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from math import ceil
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.database.connection import get_conn


class ClickHouseClient:
    def __init__(self) -> None:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = os.getenv("CLICKHOUSE_PORT", "8123")
        database = os.getenv("CLICKHOUSE_DB", "ecommerce_ods")
        self.url = f"http://{host}:{port}/?{urlencode({'database': database})}"
        self.user = os.getenv("CLICKHOUSE_USER", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.timeout = float(os.getenv("BENCHMARK_CLICKHOUSE_TIMEOUT", "10"))

    def query(self, sql: str) -> str:
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
                return response.read().decode("utf-8").strip()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"cannot connect to ClickHouse at {self.url}: {exc.reason}") from exc


def insert_probe(probe_key: str, payload: str) -> int:
    sql = """
        INSERT INTO cdc_latency_probe (probe_key, payload, source_updated_at)
        VALUES (%s, %s, clock_timestamp())
        RETURNING probe_id
    """
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (probe_key, payload))
            probe_id = int(cursor.fetchone()[0])
        conn.commit()
        return probe_id
    finally:
        conn.close()


def wait_for_probe(clickhouse: ClickHouseClient, probe_id: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    sql = f"""
        SELECT count()
        FROM ecommerce_ods.cdc_latency_probe_sink FINAL
        WHERE probe_id = {probe_id}
          AND deleted_at IS NULL
    """
    while time.monotonic() < deadline:
        if int(clickhouse.query(sql) or "0") > 0:
            return True
        time.sleep(0.1)
    return False


def percentile(samples: list[float], percent: int) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


def format_seconds(value: float) -> str:
    return f"{value:.3f}s"


def run_benchmark(samples: int, interval: float, timeout: float, probe_key: str) -> int:
    clickhouse = ClickHouseClient()
    latencies = []
    timeouts = 0

    print(
        "CDC latency benchmark "
        f"samples={samples} interval={interval:g}s timeout={timeout:g}s key={probe_key}"
    )

    for index in range(1, samples + 1):
        payload = f"{probe_key}:{index}:{time.time_ns()}"
        try:
            probe_id = insert_probe(probe_key, payload)
            committed_at = time.monotonic()
            observed = wait_for_probe(clickhouse, probe_id, timeout)
        except Exception as exc:  # noqa: BLE001 - CLI should show concise failures.
            print(f"[ERROR] sample {index}: {exc}", file=sys.stderr)
            return 2

        if observed:
            latency = time.monotonic() - committed_at
            latencies.append(latency)
            print(
                f"[{index:03d}/{samples:03d}] probe_id={probe_id} "
                f"latency={format_seconds(latency)}"
            )
        else:
            timeouts += 1
            print(f"[{index:03d}/{samples:03d}] probe_id={probe_id} latency=TIMEOUT")

        if index < samples and interval > 0:
            time.sleep(interval)

    print()
    print("| Metric | Value |")
    print("| --- | ---: |")
    print(f"| Samples requested | {samples} |")
    print(f"| Samples observed | {len(latencies)} |")
    print(f"| Timeouts | {timeouts} |")
    print(f"| p50 | {format_seconds(percentile(latencies, 50))} |")
    print(f"| p95 | {format_seconds(percentile(latencies, 95))} |")
    print(f"| p99 | {format_seconds(percentile(latencies, 99))} |")
    print(f"| max | {format_seconds(max(latencies) if latencies else 0.0)} |")

    return 0 if latencies and timeouts == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark CDC latency with probe rows.")
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--probe-key",
        default=f"cdc-latency-benchmark-{socket.gethostname()}",
        help="logical key written to cdc_latency_probe.probe_key",
    )
    args = parser.parse_args()

    if args.samples < 1:
        print("[ERROR] --samples must be at least 1", file=sys.stderr)
        return 2
    if args.interval < 0:
        print("[ERROR] --interval must be greater than or equal to 0", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("[ERROR] --timeout must be greater than 0", file=sys.stderr)
        return 2

    return run_benchmark(args.samples, args.interval, args.timeout, args.probe_key)


if __name__ == "__main__":
    raise SystemExit(main())
