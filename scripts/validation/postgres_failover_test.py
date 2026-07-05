"""Exercise Patroni PostgreSQL failover behind HAProxy.

Run from the project root:
    python -m scripts.validation.postgres_failover_test
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

from scripts.benchmark.cdc_latency_benchmark import ClickHouseClient, insert_probe, wait_for_probe
from scripts.database.connection import get_conn


PATRONI_ENDPOINTS = {
    "pg-node-1": "http://localhost:8008/",
    "pg-node-2": "http://localhost:8009/",
    "pg-node-3": "http://localhost:8010/",
}


@dataclass
class FailoverResult:
    status: str
    primary_before: str
    primary_after: str
    recovery_seconds: float
    pre_failover_probe_id: int | None
    post_failover_probe_id: int | None
    validation_status: str
    validation_tail: str
    error: str


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=check,
        text=True,
        capture_output=True,
    )


def patroni_statuses() -> dict[str, dict]:
    statuses = {}
    for node, url in PATRONI_ENDPOINTS.items():
        try:
            with urlopen(url, timeout=3) as response:
                payload = json.load(response)
        except Exception as exc:  # noqa: BLE001 - callers need node-level status.
            statuses[node] = {"name": node, "role": "down", "state": "down", "error": str(exc)}
        else:
            payload["name"] = payload.get("name") or node
            statuses[node] = payload
    return statuses


def current_primary() -> str:
    statuses = patroni_statuses()
    primaries = [
        node
        for node, status in statuses.items()
        if status.get("role") in {"primary", "master"}
    ]
    if len(primaries) != 1:
        raise RuntimeError(f"expected exactly one Patroni primary, got {statuses}")
    return primaries[0]


def wait_for_new_primary(previous_primary: str, timeout: float) -> tuple[str, float]:
    started = time.monotonic()
    deadline = started + timeout
    while time.monotonic() < deadline:
        try:
            primary = current_primary()
        except RuntimeError:
            time.sleep(1)
            continue
        if primary != previous_primary:
            return primary, time.monotonic() - started
        time.sleep(1)
    raise RuntimeError(f"no new primary promoted within {timeout:.0f}s")


def wait_for_haproxy_write(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT pg_is_in_recovery()")
                    in_recovery = cursor.fetchone()[0]
            if in_recovery is False:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("HAProxy write endpoint did not route to a writable primary")


def wait_for_flink_running(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urlopen("http://localhost:8081/jobs/overview", timeout=5) as response:
                jobs = json.load(response).get("jobs", [])
        except Exception:
            time.sleep(2)
            continue
        if any(job.get("state") == "RUNNING" for job in jobs):
            return
        time.sleep(2)
    raise RuntimeError("Flink job did not return to RUNNING")


def wait_for_slots_active(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    expected_slots = {
        "flink_customers_slot",
        "flink_categories_slot",
        "flink_products_slot",
        "flink_orders_slot",
        "flink_order_items_slot",
        "flink_payments_slot",
        "flink_shipments_slot",
        "flink_inventory_movements_slot",
        "flink_cdc_latency_probe_slot",
    }
    while time.monotonic() < deadline:
        try:
            with get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT slot_name FROM pg_replication_slots WHERE slot_type='logical' AND active"
                    )
                    active_slots = {row[0] for row in cursor.fetchall()}
            if expected_slots.issubset(active_slots):
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Logical replication slots did not become active on the new primary")


def write_probe(clickhouse: ClickHouseClient, probe_key: str, timeout: float) -> int:
    probe_id = insert_probe(probe_key, f"{probe_key}:{time.time_ns()}")
    if not wait_for_probe(clickhouse, probe_id, timeout):
        raise RuntimeError(f"probe_id={probe_id} did not reach ClickHouse within {timeout:.0f}s")
    return probe_id


def run_validation() -> tuple[str, str]:
    env = os.environ.copy()
    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("POSTGRES_PORT", "15432")
    result = subprocess.run(
        [sys.executable, "-m", "scripts.validation.compare_postgres_clickhouse"],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    output = "\n".join(part for part in (result.stdout, result.stderr) if part.strip())
    tail = "\n".join(output.splitlines()[-40:])
    return ("PASS" if result.returncode == 0 else "FAIL", tail)


def write_report(path: Path, result: FailoverResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# PostgreSQL HA failover result",
                "",
                f"- Status: {result.status}",
                f"- Primary before: {result.primary_before}",
                f"- Primary after: {result.primary_after}",
                f"- Recovery time: {result.recovery_seconds:.2f}s",
                f"- Pre-failover probe id: {result.pre_failover_probe_id}",
                f"- Post-failover probe id: {result.post_failover_probe_id}",
                f"- Validation: {result.validation_status}",
                "",
                "## Validation tail",
                "",
                "```text",
                result.validation_tail or "n/a",
                "```",
                "",
                "## Error",
                "",
                "```text",
                result.error or "n/a",
                "```",
                "",
                "## Note",
                "",
                "This test proves Patroni promotion, HAProxy role-aware routing, Flink job availability, "
                "and CDC probe propagation for the local workload. Logical replication slot behavior across "
                "failover remains the key production risk to keep validating with larger workloads.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_test(args: argparse.Namespace) -> FailoverResult:
    clickhouse = ClickHouseClient()
    probe_key = f"postgres-ha-failover-{socket.gethostname()}-{int(time.time())}"
    stopped_primary = ""
    primary_before = ""
    primary_after = ""
    recovery_seconds = 0.0
    pre_probe_id: int | None = None
    post_probe_id: int | None = None
    validation_status = "SKIPPED" if args.skip_validation else "NOT_RUN"
    validation_tail = ""
    error = ""

    try:
        primary_before = current_primary()
        wait_for_haproxy_write(args.timeout)
        wait_for_flink_running(args.timeout)
        pre_probe_id = write_probe(clickhouse, f"{probe_key}-before", args.probe_timeout)

        stopped_primary = primary_before
        docker("stop", stopped_primary)
        primary_after, recovery_seconds = wait_for_new_primary(primary_before, args.timeout)
        wait_for_haproxy_write(args.timeout)
        wait_for_flink_running(args.timeout)
        wait_for_slots_active(args.timeout)

        post_probe_id = write_probe(clickhouse, f"{probe_key}-after", args.probe_timeout)
        if not args.skip_validation:
            validation_status, validation_tail = run_validation()
            if validation_status != "PASS":
                raise RuntimeError("validation failed after PostgreSQL failover")

        status = "PASS"
    except Exception as exc:  # noqa: BLE001 - CLI should report and still restore node.
        status = "FAIL"
        error = str(exc) or exc.__class__.__name__
    finally:
        if stopped_primary:
            docker("start", stopped_primary, check=False)

    return FailoverResult(
        status=status,
        primary_before=primary_before or "unknown",
        primary_after=primary_after or "unknown",
        recovery_seconds=recovery_seconds,
        pre_failover_probe_id=pre_probe_id,
        post_failover_probe_id=post_probe_id,
        validation_status=validation_status,
        validation_tail=validation_tail,
        error=error,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Patroni PostgreSQL HA failover test.")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--probe-timeout", type=float, default=120.0)
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--report-file", type=Path)
    args = parser.parse_args()

    result = run_test(args)
    if args.report_file:
        write_report(args.report_file, result)

    print(f"status={result.status}")
    print(f"primary_before={result.primary_before}")
    print(f"primary_after={result.primary_after}")
    print(f"recovery_seconds={result.recovery_seconds:.2f}")
    print(f"validation={result.validation_status}")
    if result.error:
        print(f"error={result.error}", file=sys.stderr)
    return 0 if result.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
