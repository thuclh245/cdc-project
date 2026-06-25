"""Verify the runtime invariants required by the multi-table CDC stack."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.request


EXPECTED_PG_TABLES = {
    "customers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory_movements",
}
EXPECTED_CH_TABLES = {f"{name}_sink" for name in EXPECTED_PG_TABLES}
EXPECTED_SLOTS = {f"flink_{name}_slot" for name in EXPECTED_PG_TABLES}


def run(*args: str) -> str:
    result = subprocess.run(args, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def get_json(path: str) -> dict:
    with urllib.request.urlopen(f"http://localhost:8081{path}", timeout=5) as response:
        return json.load(response)


def lines(value: str) -> set[str]:
    return {line.strip() for line in value.splitlines() if line.strip()}


def assert_equal(label: str, actual: set[str], expected: set[str]) -> None:
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RuntimeError(f"{label}: missing={missing}, unexpected={extra}")
    print(f"[OK] {label}: {len(actual)}")


def verify_tables() -> None:
    pg_tables = lines(
        run(
            "docker", "exec", "pg-primary", "psql", "-U", "postgres",
            "-d", "ecommerce_ods", "-Atc",
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1",
        )
    )
    ch_tables = lines(
        run(
            "docker", "exec", "clickhouse-sink", "clickhouse-client", "--query",
            "SELECT name FROM system.tables WHERE database='ecommerce_ods' ORDER BY name",
        )
    )
    assert_equal("PostgreSQL application tables", pg_tables, EXPECTED_PG_TABLES)
    assert_equal("ClickHouse sink tables", ch_tables, EXPECTED_CH_TABLES)


def verify_jobs_and_slots(timeout: int = 90) -> str:
    deadline = time.monotonic() + timeout
    last_states: list[tuple[str | None, str | None]] = []
    last_slots: set[str] = set()
    while time.monotonic() < deadline:
        jobs = get_json("/jobs/overview").get("jobs", [])
        running = [job for job in jobs if job.get("state") == "RUNNING"]
        last_states = [(job.get("name"), job.get("state")) for job in jobs]
        last_slots = lines(
            run(
                "docker", "exec", "pg-primary", "psql", "-U", "postgres",
                "-d", "ecommerce_ods", "-Atc",
                "SELECT slot_name FROM pg_replication_slots "
                "WHERE slot_type='logical' AND active ORDER BY slot_name",
            )
        )
        if len(running) == 1 and last_slots == EXPECTED_SLOTS:
            print(f"[OK] Flink job RUNNING: {running[0].get('name')}")
            assert_equal("active logical replication slots", last_slots, EXPECTED_SLOTS)
            return running[0]["jid"]
        time.sleep(5)
    raise RuntimeError(
        f"Flink job/slots not ready within {timeout}s: "
        f"jobs={last_states}, active_slots={sorted(last_slots)}"
    )


def verify_checkpoints(job_id: str, timeout: int = 360) -> None:
    deadline = time.monotonic() + timeout
    initial = get_json(f"/jobs/{job_id}/checkpoints").get("counts", {}).get("completed", 0)
    while time.monotonic() < deadline:
        completed = get_json(f"/jobs/{job_id}/checkpoints").get("counts", {}).get("completed", 0)
        if completed >= 2 and completed > initial:
            print(f"[OK] checkpoints continue completing: {initial} -> {completed}")
            return
        time.sleep(5)
    raise RuntimeError(f"no new completed checkpoint within {timeout}s (initial={initial})")


def verify_logs() -> None:
    logs = run(
        "docker", "compose", "logs", "--no-color", "flink-jobmanager", "flink-taskmanager"
    )
    markers = ("Caused by:", "Exception in thread", "[ERROR]", " ERROR ")
    matches = []
    for line in logs.splitlines():
        # A browser tab may keep polling a job ID from before `make reset`.
        # Flink logs that harmless 404 as ERROR, although the current job is healthy.
        stale_ui_poll = "runtime.rest.handler.job" in line and " not found" in line
        if not stale_ui_poll and any(marker in line for marker in markers):
            matches.append(line)
    if matches:
        preview = "\n".join(matches[-10:])
        raise RuntimeError(f"Flink logs contain error/exception markers:\n{preview}")
    print("[OK] no error/exception markers in JobManager/TaskManager logs")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--readiness",
        action="store_true",
        help="check bootstrap readiness without requiring traffic-driven checkpoints",
    )
    args = parser.parse_args()

    verify_tables()
    job_id = verify_jobs_and_slots()
    verify_logs()
    if args.readiness:
        print("CDC stack is ready for seed traffic.")
    else:
        verify_checkpoints(job_id)
        print("CDC stack verification passed.")


if __name__ == "__main__":
    main()
