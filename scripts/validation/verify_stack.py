"""Verify the runtime invariants required by the multi-table CDC stack."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
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
MIN_SEEDED_ROWS = {
    "customers": 1,
    "categories": 1,
    "products": 1,
    "orders": 1,
    "order_items": 1,
    "payments": 1,
    "shipments": 1,
    "inventory_movements": 1,
}


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


def verify_seed_data() -> None:
    query = (
        "SELECT table_name || ':' || row_count FROM ("
        + " UNION ALL ".join(
            f"SELECT '{table}' AS table_name, COUNT(*) AS row_count FROM {table}"
            for table in sorted(MIN_SEEDED_ROWS)
        )
        + ") counts ORDER BY table_name"
    )
    counts = {}
    for row in lines(
        run(
            "docker", "exec", "pg-primary", "psql", "-U", "postgres",
            "-d", "ecommerce_ods", "-Atc", query,
        )
    ):
        table, count = row.split(":", 1)
        counts[table] = int(count)

    empty_tables = [
        table for table, minimum in sorted(MIN_SEEDED_ROWS.items())
        if counts.get(table, 0) < minimum
    ]
    if empty_tables:
        details = ", ".join(f"{table}={counts.get(table, 0)}" for table in empty_tables)
        raise RuntimeError(
            "PostgreSQL CDC source tables need seed data before checkpoint verification: "
            f"{details}. Run `make seed` first."
        )
    print(f"[OK] seeded PostgreSQL CDC source tables: {len(counts)}")


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
    path = f"/jobs/{job_id}/checkpoints"
    initial = get_json(path).get("counts", {}).get("completed", 0)
    while time.monotonic() < deadline:
        completed = get_json(path).get("counts", {}).get("completed", 0)
        if completed >= 2 and completed > initial:
            print(f"[OK] checkpoints continue completing: {initial} -> {completed}")
            return
        time.sleep(5)
    checkpoints = get_json(path)
    detail = format_checkpoint_diagnosis(job_id, checkpoints)
    raise RuntimeError(
        f"no new completed checkpoint within {timeout}s (initial={initial})\n{detail}"
    )


def format_checkpoint_diagnosis(job_id: str, checkpoints: dict) -> str:
    counts = checkpoints.get("counts", {})
    latest = checkpoints.get("latest", {})
    failed = latest.get("failed") or {}
    in_progress = latest.get("in_progress") or {}
    checkpoint = checkpoint_detail(job_id, checkpoints)
    job = get_json(f"/jobs/{job_id}")
    vertex_names = {vertex["id"]: vertex["name"] for vertex in job.get("vertices", [])}
    lines_out = [
        "Checkpoint diagnosis:",
        (
            "  counts: "
            f"triggered={counts.get('total', 0)}, "
            f"in_progress={counts.get('in_progress', 0)}, "
            f"completed={counts.get('completed', 0)}, "
            f"failed={counts.get('failed', 0)}"
        ),
    ]
    if failed:
        lines_out.append(
            "  latest failed: "
            f"id={failed.get('id')}, cause={failed.get('failure_message', 'unknown')}"
        )
    elif in_progress:
        lines_out.append(f"  in progress: id={in_progress.get('id')}")
    else:
        lines_out.append("  latest checkpoint detail is not available from Flink REST API")

    unacked = []
    for vertex_id, task in checkpoint.get("tasks", {}).items():
        name = vertex_names.get(vertex_id, vertex_id)
        latest_ack = task.get("latest_ack_timestamp", 0)
        acknowledgements = task.get("num_acknowledged_subtasks")
        total = task.get("num_subtasks")
        if total is not None and acknowledgements != total:
            unacked.append((name, acknowledgements, total, latest_ack))
        elif latest_ack in (0, -1, None):
            unacked.append((name, acknowledgements, total, latest_ack))

    if unacked:
        lines_out.append("  operators not fully acknowledged:")
        for name, acknowledgements, total, latest_ack in unacked[:8]:
            ack_text = "unknown"
            if acknowledgements is not None and total is not None:
                ack_text = f"{acknowledgements}/{total}"
            lines_out.append(f"    - {name}: ack={ack_text}, latest_ack={latest_ack}")
    else:
        lines_out.append("  no unacknowledged operator reported by Flink REST API")

    lines_out.extend(
        [
            "Next checks:",
            "  - Open http://localhost:8081 and inspect Checkpoints -> History.",
            "  - If make ready and make validate pass, CDC data is consistent; this is checkpoint health.",
            "  - Restart the stack after checkpoint config changes: make reset && make seed && make verify.",
        ]
    )
    return "\n".join(lines_out)


def checkpoint_detail(job_id: str, checkpoints: dict) -> dict:
    candidates = [
        checkpoints.get("latest", {}).get("in_progress"),
        checkpoints.get("latest", {}).get("failed"),
    ]
    candidates.extend(checkpoints.get("history", []))
    for checkpoint in candidates:
        if not checkpoint or checkpoint.get("id") is None:
            continue
        detail = get_json(f"/jobs/{job_id}/checkpoints/details/{checkpoint['id']}")
        if detail.get("tasks"):
            return detail
    return candidates[0] or {}


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
    verify_seed_data()
    job_id = verify_jobs_and_slots()
    verify_logs()
    if args.readiness:
        print("CDC stack is ready for seed traffic.")
    else:
        verify_checkpoints(job_id)
        print("CDC stack verification passed.")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        raise SystemExit(1) from None
