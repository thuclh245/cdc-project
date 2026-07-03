"""Expose CDC validation results as Prometheus metrics."""

from __future__ import annotations

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from math import ceil
from urllib.request import urlopen

from scripts.database.connection import get_conn
from scripts.validation.compare_postgres_clickhouse import (
    CHECKS,
    ClickHouseHttpClient,
    run_checks_once,
)


CHECK_NAMES = [check.name for check in CHECKS]
CDC_TABLES = (
    "customers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory_movements",
)
CHECK_LABELS = {
    name: name.lower()
    .replace(" ", "_")
    .replace("/", "_")
    .replace("-", "_")
    for name in CHECK_NAMES
}

STATE = {
    "last_run": 0.0,
    "last_duration": 0.0,
    "up": 0,
    "failed_checks": len(CHECKS),
    "checks": {name: 0 for name in CHECK_NAMES},
    "source_tables": {table: 0 for table in CDC_TABLES},
    "source_table_seeded": {table: 0 for table in CDC_TABLES},
    "replication_slots": {},
    "active_replication_slots": 0,
    "patroni_nodes": {},
    "patroni_primary_count": 0,
    "latency_up": 0,
    "latency_error": "",
    "latency_samples": [],
    "latency_samples_window": 0,
    "latency_last": 0.0,
    "latency_p50": 0.0,
    "latency_p95": 0.0,
    "latency_p99": 0.0,
    "latency_max": 0.0,
    "error": "",
}


def render_metrics() -> str:
    now = time.time()
    interval = float(os.getenv("CDC_VALIDATION_INTERVAL_SECONDS", "30"))

    if now - STATE["last_run"] >= interval:
        refresh_state()

    lines = [
        "# HELP cdc_validation_up Whether the validation exporter ran successfully.",
        "# TYPE cdc_validation_up gauge",
        f"cdc_validation_up {STATE['up']}",
        "# HELP cdc_validation_pass Whether all CDC validation checks passed.",
        "# TYPE cdc_validation_pass gauge",
        f"cdc_validation_pass {1 if STATE['failed_checks'] == 0 and STATE['up'] else 0}",
        "# HELP cdc_validation_failed_checks Number of failed CDC validation checks.",
        "# TYPE cdc_validation_failed_checks gauge",
        f"cdc_validation_failed_checks {STATE['failed_checks']}",
        "# HELP cdc_validation_last_run_timestamp_seconds Unix timestamp of the last validation run.",
        "# TYPE cdc_validation_last_run_timestamp_seconds gauge",
        f"cdc_validation_last_run_timestamp_seconds {STATE['last_run']:.0f}",
        "# HELP cdc_validation_last_duration_seconds Duration of the last validation run.",
        "# TYPE cdc_validation_last_duration_seconds gauge",
        f"cdc_validation_last_duration_seconds {STATE['last_duration']:.6f}",
        "# HELP cdc_validation_check_pass Per-check CDC validation status.",
        "# TYPE cdc_validation_check_pass gauge",
    ]

    for name in CHECK_NAMES:
        label = CHECK_LABELS[name]
        lines.append(f'cdc_validation_check_pass{{check="{label}"}} {STATE["checks"][name]}')

    lines.extend(
        [
            "# HELP cdc_source_table_rows PostgreSQL source table row count.",
            "# TYPE cdc_source_table_rows gauge",
        ]
    )
    for table, rows in STATE["source_tables"].items():
        lines.append(f'cdc_source_table_rows{{table="{table}"}} {rows}')

    lines.extend(
        [
            "# HELP cdc_source_table_seeded Whether the PostgreSQL source table contains at least one row.",
            "# TYPE cdc_source_table_seeded gauge",
        ]
    )
    for table, seeded in STATE["source_table_seeded"].items():
        lines.append(f'cdc_source_table_seeded{{table="{table}"}} {seeded}')

    lines.extend(
        [
            "# HELP cdc_postgres_active_logical_replication_slots Number of active logical replication slots.",
            "# TYPE cdc_postgres_active_logical_replication_slots gauge",
            f"cdc_postgres_active_logical_replication_slots {STATE['active_replication_slots']}",
            "# HELP cdc_postgres_replication_slot_active Whether a logical replication slot is active.",
            "# TYPE cdc_postgres_replication_slot_active gauge",
        ]
    )
    for slot, values in STATE["replication_slots"].items():
        lines.append(
            f'cdc_postgres_replication_slot_active{{slot="{slot}"}} {values["active"]}'
        )

    lines.extend(
        [
            "# HELP cdc_postgres_replication_slot_lag_bytes WAL bytes retained for each logical replication slot.",
            "# TYPE cdc_postgres_replication_slot_lag_bytes gauge",
        ]
    )
    for slot, values in STATE["replication_slots"].items():
        lines.append(
            f'cdc_postgres_replication_slot_lag_bytes{{slot="{slot}"}} {values["lag_bytes"]}'
        )

    lines.extend(
        [
            "# HELP cdc_postgres_patroni_node_up Whether the Patroni REST API is reachable for a node.",
            "# TYPE cdc_postgres_patroni_node_up gauge",
        ]
    )
    for node, values in STATE["patroni_nodes"].items():
        lines.append(
            f'cdc_postgres_patroni_node_up{{node="{node}",role="{values["role"]}"}} {values["up"]}'
        )

    lines.extend(
        [
            "# HELP cdc_postgres_patroni_role_primary Whether a Patroni node currently reports primary role.",
            "# TYPE cdc_postgres_patroni_role_primary gauge",
        ]
    )
    for node, values in STATE["patroni_nodes"].items():
        lines.append(
            f'cdc_postgres_patroni_role_primary{{node="{node}"}} {values["primary"]}'
        )

    lines.extend(
        [
            "# HELP cdc_postgres_patroni_role_replica Whether a Patroni node currently reports replica role.",
            "# TYPE cdc_postgres_patroni_role_replica gauge",
        ]
    )
    for node, values in STATE["patroni_nodes"].items():
        lines.append(
            f'cdc_postgres_patroni_role_replica{{node="{node}"}} {values["replica"]}'
        )

    lines.extend(
        [
            "# HELP cdc_postgres_patroni_primary_count Number of Patroni nodes reporting primary role.",
            "# TYPE cdc_postgres_patroni_primary_count gauge",
            f"cdc_postgres_patroni_primary_count {STATE['patroni_primary_count']}",
        ]
    )

    lines.extend(
        [
            "# HELP cdc_latency_up Whether latency probe metrics refreshed successfully.",
            "# TYPE cdc_latency_up gauge",
            f"cdc_latency_up {STATE['latency_up']}",
            "# HELP cdc_latency_last_seconds CDC latency for the latest newly observed probe sample.",
            "# TYPE cdc_latency_last_seconds gauge",
            f"cdc_latency_last_seconds {STATE['latency_last']:.6f}",
            "# HELP cdc_latency_p50_seconds Median CDC latency over samples seen by this exporter.",
            "# TYPE cdc_latency_p50_seconds gauge",
            f"cdc_latency_p50_seconds {STATE['latency_p50']:.6f}",
            "# HELP cdc_latency_p95_seconds 95th percentile CDC latency over samples seen by this exporter.",
            "# TYPE cdc_latency_p95_seconds gauge",
            f"cdc_latency_p95_seconds {STATE['latency_p95']:.6f}",
            "# HELP cdc_latency_p99_seconds 99th percentile CDC latency over samples seen by this exporter.",
            "# TYPE cdc_latency_p99_seconds gauge",
            f"cdc_latency_p99_seconds {STATE['latency_p99']:.6f}",
            "# HELP cdc_latency_max_seconds Max CDC latency over samples seen by this exporter.",
            "# TYPE cdc_latency_max_seconds gauge",
            f"cdc_latency_max_seconds {STATE['latency_max']:.6f}",
            "# HELP cdc_latency_samples_window Latency probe samples in the exporter lookback window.",
            "# TYPE cdc_latency_samples_window gauge",
            f"cdc_latency_samples_window {STATE['latency_samples_window']}",
        ]
    )

    return "\n".join(lines) + "\n"


def refresh_state() -> None:
    started = time.monotonic()
    STATE["last_run"] = time.time()
    patroni_nodes = load_patroni_nodes()
    STATE["patroni_nodes"] = patroni_nodes
    STATE["patroni_primary_count"] = sum(
        values["primary"] for values in patroni_nodes.values()
    )
    try:
        mismatches = run_checks_once()
        source_tables = load_source_table_counts()
        replication_slots = load_replication_slots()
    except Exception as exc:  # noqa: BLE001 - exporter should report, not crash.
        STATE["up"] = 0
        STATE["failed_checks"] = len(CHECKS)
        STATE["checks"] = {name: 0 for name in CHECK_NAMES}
        STATE["source_tables"] = {table: 0 for table in CDC_TABLES}
        STATE["source_table_seeded"] = {table: 0 for table in CDC_TABLES}
        STATE["replication_slots"] = {}
        STATE["active_replication_slots"] = 0
        STATE["error"] = str(exc) or exc.__class__.__name__
    else:
        failed = {check.name for check, _, _ in mismatches}
        STATE["up"] = 1
        STATE["failed_checks"] = len(failed)
        STATE["checks"] = {name: 0 if name in failed else 1 for name in CHECK_NAMES}
        STATE["source_tables"] = source_tables
        STATE["source_table_seeded"] = {
            table: 1 if count > 0 else 0 for table, count in source_tables.items()
        }
        STATE["replication_slots"] = replication_slots
        STATE["active_replication_slots"] = sum(
            values["active"] for values in replication_slots.values()
        )
        STATE["error"] = ""
        refresh_latency_state()
    finally:
        STATE["last_duration"] = time.monotonic() - started


def load_source_table_counts() -> dict[str, int]:
    sql = (
        "SELECT table_name, row_count FROM ("
        + " UNION ALL ".join(
            f"SELECT '{table}' AS table_name, COUNT(*) AS row_count FROM {table}"
            for table in CDC_TABLES
        )
        + ") counts ORDER BY table_name"
    )
    postgres = get_conn()
    try:
        with postgres.cursor() as cursor:
            cursor.execute(sql)
            return {table: int(count) for table, count in cursor.fetchall()}
    finally:
        postgres.close()


def load_replication_slots() -> dict[str, dict[str, int]]:
    sql = """
        SELECT
            slot_name,
            CASE WHEN active THEN 1 ELSE 0 END AS active,
            COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint AS lag_bytes
        FROM pg_replication_slots
        WHERE slot_type = 'logical'
        ORDER BY slot_name
    """
    postgres = get_conn()
    try:
        with postgres.cursor() as cursor:
            cursor.execute(sql)
            return {
                slot_name: {
                    "active": int(active),
                    "lag_bytes": int(lag_bytes),
                }
                for slot_name, active, lag_bytes in cursor.fetchall()
            }
    finally:
        postgres.close()


def load_patroni_nodes() -> dict[str, dict[str, int | str]]:
    raw_nodes = os.getenv(
        "PATRONI_NODES",
        "pg-node-1:8008,pg-node-2:8008,pg-node-3:8008",
    )
    nodes: dict[str, dict[str, int | str]] = {}
    for target in [value.strip() for value in raw_nodes.split(",") if value.strip()]:
        node = target.split(":", 1)[0]
        try:
            with urlopen(f"http://{target}/", timeout=3) as response:
                payload = json.load(response)
        except Exception:  # noqa: BLE001 - metrics should report node down.
            nodes[node] = {
                "up": 0,
                "role": "down",
                "primary": 0,
                "replica": 0,
            }
            continue

        role = str(payload.get("role") or "unknown")
        normalized_role = "primary" if role == "master" else role
        name = str(payload.get("name") or node)
        nodes[name] = {
            "up": 1,
            "role": normalized_role,
            "primary": 1 if normalized_role == "primary" else 0,
            "replica": 1 if normalized_role == "replica" else 0,
        }
    return nodes


def refresh_latency_state() -> None:
    try:
        new_samples = load_new_latency_samples()
    except Exception as exc:  # noqa: BLE001 - latency should not break validation.
        STATE["latency_up"] = 0
        STATE["latency_error"] = str(exc) or exc.__class__.__name__
        return

    STATE["latency_samples"] = new_samples
    STATE["latency_samples_window"] = len(new_samples)
    STATE["latency_last"] = new_samples[-1] if new_samples else 0.0

    samples = STATE["latency_samples"]
    STATE["latency_up"] = 1
    STATE["latency_error"] = ""
    STATE["latency_p50"] = percentile(samples, 50)
    STATE["latency_p95"] = percentile(samples, 95)
    STATE["latency_p99"] = percentile(samples, 99)
    STATE["latency_max"] = max(samples) if samples else 0.0


def load_new_latency_samples() -> list[float]:
    lookback_seconds = int(os.getenv("CDC_LATENCY_EXPORTER_LOOKBACK_SECONDS", "3600"))
    sql = f"""
        SELECT
            probe_id,
            greatest(
                0,
                toUnixTimestamp64Micro(sink_observed_at)
                - toUnixTimestamp64Micro(source_updated_at)
            ) / 1000000
        FROM ecommerce_ods.cdc_latency_probe_sink FINAL
        WHERE deleted_at IS NULL
          AND sink_observed_at > source_updated_at
          AND source_updated_at >= now64(6) - INTERVAL {lookback_seconds} SECOND
        ORDER BY probe_id
    """
    raw = ClickHouseHttpClient().scalar(sql, lambda value: value.strip())

    if not raw:
        return []

    samples = [float(line.split("\t", 1)[1]) for line in raw.splitlines()]
    STATE["latency_samples_window"] = len(samples)
    return samples


def percentile(samples: list[float], percent: int) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API.
        if self.path not in ("/", "/metrics"):
            self.send_response(404)
            self.end_headers()
            return

        body = render_metrics().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    host = os.getenv("CDC_VALIDATION_EXPORTER_HOST", "0.0.0.0")
    port = int(os.getenv("CDC_VALIDATION_EXPORTER_PORT", "9108"))
    refresh_state()
    server = ThreadingHTTPServer((host, port), MetricsHandler)
    print(f"CDC validation exporter listening on {host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
