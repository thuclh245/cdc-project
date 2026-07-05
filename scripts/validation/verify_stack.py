"""Verify the runtime invariants required by the multi-table CDC stack."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from urllib.error import HTTPError


EXPECTED_PG_TABLES = {
    "cdc_latency_probe",
    "customers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory_movements",
}
EXPECTED_PG_SUPPORT_TABLES = {
    "cdc_validation_runs",
    "cdc_validation_check_results",
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
PG_CLI_CONTAINER = os.getenv("POSTGRES_CLI_CONTAINER", "pg-node-1")
PG_SERVICE_HOST = os.getenv("POSTGRES_SERVICE_HOST", "pg-haproxy")
PG_WRITE_PORT = os.getenv("POSTGRES_SERVICE_WRITE_PORT", "5432")
PG_READ_PORT = os.getenv("POSTGRES_SERVICE_READ_PORT", "5433")
PATRONI_API_ENDPOINTS = tuple(
    endpoint.strip()
    for endpoint in os.getenv(
        "PATRONI_API_ENDPOINTS",
        "localhost:8008,localhost:8009,localhost:8010",
    ).split(",")
    if endpoint.strip()
)


def run(*args: str) -> str:
    result = subprocess.run(args, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def get_json(path: str) -> dict:
    with urllib.request.urlopen(f"http://localhost:8081{path}", timeout=5) as response:
        return json.load(response)


def get_url_json(url: str, timeout: float = 5.0) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise


def lines(value: str) -> set[str]:
    return {line.strip() for line in value.splitlines() if line.strip()}


def assert_equal(label: str, actual: set[str], expected: set[str]) -> None:
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RuntimeError(f"{label}: missing={missing}, unexpected={extra}")
    print(f"[OK] {label}: {len(actual)}")


def assert_postgres_tables(actual: set[str]) -> None:
    missing = sorted(EXPECTED_PG_TABLES - actual)
    unexpected = sorted(actual - EXPECTED_PG_TABLES - EXPECTED_PG_SUPPORT_TABLES)
    if missing or unexpected:
        raise RuntimeError(
            f"PostgreSQL tables: missing={missing}, unexpected={unexpected}"
        )
    support_count = len(actual & EXPECTED_PG_SUPPORT_TABLES)
    print(
        "[OK] PostgreSQL tables: "
        f"{len(actual & EXPECTED_PG_TABLES)} CDC source, {support_count} support"
    )


def run_psql(sql: str, *, port: str = PG_WRITE_PORT) -> str:
    return run(
        "docker", "exec", "-e", "PGPASSWORD=postgres", PG_CLI_CONTAINER, "psql",
        "-h", PG_SERVICE_HOST,
        "-p", port,
        "-U", "postgres",
        "-d", "ecommerce_ods",
        "-Atc", sql,
    )


def patroni_statuses() -> list[dict[str, str]]:
    statuses = []
    for endpoint in PATRONI_API_ENDPOINTS:
        url = f"http://{endpoint}/"
        data = get_url_json(url)
        statuses.append(
            {
                "endpoint": endpoint,
                "name": str(data.get("name") or endpoint),
                "role": str(data.get("role") or "unknown"),
                "state": str(data.get("state") or "unknown"),
            }
        )
    return statuses


def verify_postgres_ha() -> None:
    statuses = patroni_statuses()
    primaries = [status for status in statuses if status["role"] in {"primary", "master"}]
    replicas = [status for status in statuses if status["role"] == "replica"]
    if len(primaries) != 1 or len(replicas) < 2:
        raise RuntimeError(f"Patroni roles are not healthy: {statuses}")

    write_recovery = run_psql("SELECT pg_is_in_recovery()")
    if write_recovery != "f":
        raise RuntimeError(
            f"HAProxy write endpoint is not routed to primary: pg_is_in_recovery={write_recovery}"
        )

    read_recovery = run_psql("SELECT pg_is_in_recovery()", port=PG_READ_PORT)
    if read_recovery != "t":
        raise RuntimeError(
            f"HAProxy read endpoint is not routed to a replica: pg_is_in_recovery={read_recovery}"
        )

    primary = primaries[0]["name"]
    print(f"[OK] Patroni PostgreSQL HA: primary={primary}, replicas={len(replicas)}")
    print("[OK] HAProxy write/read endpoints route to primary/replica roles")


def verify_tables() -> None:
    pg_tables = lines(
        run_psql("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1")
    )
    ch_tables = lines(
        run(
            "docker", "exec", "clickhouse-sink", "clickhouse-client", "--query",
            "SELECT name FROM system.tables WHERE database='ecommerce_ods' ORDER BY name",
        )
    )
    assert_postgres_tables(pg_tables)
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
        run_psql(query)
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


def verify_minio() -> None:
    status = run(
        "docker", "inspect", "--format",
        "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}",
        "minio",
    )
    if status != "healthy":
        raise RuntimeError(f"MinIO service is not healthy: status={status}")

    run(
        "docker", "compose", "run", "--rm", "--entrypoint", "/bin/sh",
        "minio-init", "-c",
        (
            'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" '
            '"$MINIO_ROOT_PASSWORD" >/dev/null && '
            'mc ls "local/$MINIO_FLINK_BUCKET" >/dev/null'
        ),
    )
    print("[OK] MinIO healthy and flink-state bucket exists")


def verify_jobs_and_slots(timeout: int = 90) -> tuple[str, int]:
    deadline = time.monotonic() + timeout
    last_states: list[tuple[str | None, str | None]] = []
    last_slots: set[str] = set()
    while time.monotonic() < deadline:
        jobs = get_json("/jobs/overview").get("jobs", [])
        running = [job for job in jobs if job.get("state") == "RUNNING"]
        last_states = [(job.get("name"), job.get("state")) for job in jobs]
        last_slots = lines(
            run_psql(
                "SELECT slot_name FROM pg_replication_slots "
                "WHERE slot_type='logical' AND active ORDER BY slot_name"
            )
        )
        if len(running) == 1 and last_slots == EXPECTED_SLOTS:
            print(f"[OK] Flink job RUNNING: {running[0].get('name')}")
            assert_equal("active logical replication slots", last_slots, EXPECTED_SLOTS)
            return running[0]["jid"], int(running[0].get("last-modification") or 0)
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


def verify_logs(since_ms: int | None = None) -> None:
    logs_args = ["docker", "logs"]
    if since_ms:
        # Flink's last-modification timestamp moves when the job recovers.
        # Give the transition a few seconds so recovered restart noise does not
        # keep poisoning readiness checks.
        logs_args.extend(["--since", str((since_ms // 1000) + 5)])
    logs = "\n".join(
        run(*logs_args, container)
        for container in ("flink-jobmanager", "flink-taskmanager")
    )
    markers = ("Caused by:", "Exception in thread", "[ERROR]", " ERROR ")
    matches = []
    for line in logs.splitlines():
        # A browser tab may keep polling a job ID from before `make reset`.
        # Flink logs that harmless 404 as ERROR, although the current job is healthy.
        stale_ui_poll = "runtime.rest.handler.job" in line and " not found" in line
        # Intentional fault-tolerance tests restart JobManager/TaskManager and
        # leave a transient Pekko association failure in the logs. Runtime and
        # slot checks above prove the current job recovered.
        transient_flink_restart = (
            (
                "ReliableDeliverySupervisor" in line
                and "Association with remote system" in line
                and "Connection refused" in line
            )
            or "FlinkExpectedException: The TaskExecutor is shutting down" in line
            or (
                "DefaultJobLeaderService" in line
                and "Registration at JobManager failed" in line
            )
            or (
                "EndpointNotStartedException" in line
                and "rpc endpoint" in line
                and "has not been started yet" in line
            )
            or (
                "rejected the registration for job" in line
                and "not responsible for job" in line
            )
            or "TaskManager used outdated connection information" in line
            or (
                "ConsoleAppender" in line
                and "closed classloader" in line
            )
            or (
                "ConsoleAppender" in line
                and "No factory method found" in line
            )
            or "Null object returned for CONSOLE in Appenders" in line
            or 'Unable to locate appender "ConsoleAppender"' in line
            or "terminating connection due to administrator command" in line
            or "Unexpected error while closing Postgres connection" in line
            or "Broken pipe (Write failed)" in line
            or "Producer failure" in line
        )
        if (
            not stale_ui_poll
            and not transient_flink_restart
            and any(marker in line for marker in markers)
        ):
            matches.append(line)
    if matches:
        preview = "\n".join(matches[-10:])
        raise RuntimeError(f"Flink logs contain error/exception markers:\n{preview}")
    print("[OK] no error/exception markers in JobManager/TaskManager logs")


def verify_checkpoint_objects() -> None:
    output = run(
        "docker", "compose", "run", "--rm", "--entrypoint", "/bin/sh",
        "minio-init", "-c",
        (
            'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" '
            '"$MINIO_ROOT_PASSWORD" >/dev/null && '
            'mc ls --recursive "local/$MINIO_FLINK_BUCKET/checkpoints"'
        ),
    )
    objects = lines(output)
    if not objects:
        raise RuntimeError("no checkpoint objects found in MinIO under checkpoints/")
    print(f"[OK] MinIO checkpoint objects visible: {len(objects)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--readiness",
        action="store_true",
        help="check bootstrap readiness without requiring traffic-driven checkpoints",
    )
    args = parser.parse_args()

    verify_postgres_ha()
    verify_tables()
    verify_minio()
    job_id, last_modification_ms = verify_jobs_and_slots()
    verify_logs(last_modification_ms)
    if args.readiness:
        print("CDC stack is ready for seed traffic.")
    else:
        verify_seed_data()
        verify_checkpoints(job_id)
        verify_checkpoint_objects()
        print("CDC stack verification passed.")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        raise SystemExit(1) from None
