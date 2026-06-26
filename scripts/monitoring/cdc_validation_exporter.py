"""Expose CDC validation results as Prometheus metrics."""

from __future__ import annotations

import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from scripts.validation.compare_postgres_clickhouse import CHECKS, run_checks_once


CHECK_NAMES = [check.name for check in CHECKS]
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

    return "\n".join(lines) + "\n"


def refresh_state() -> None:
    started = time.monotonic()
    STATE["last_run"] = time.time()
    try:
        mismatches = run_checks_once()
    except Exception as exc:  # noqa: BLE001 - exporter should report, not crash.
        STATE["up"] = 0
        STATE["failed_checks"] = len(CHECKS)
        STATE["checks"] = {name: 0 for name in CHECK_NAMES}
        STATE["error"] = str(exc) or exc.__class__.__name__
    else:
        failed = {check.name for check, _, _ in mismatches}
        STATE["up"] = 1
        STATE["failed_checks"] = len(failed)
        STATE["checks"] = {name: 0 if name in failed else 1 for name in CHECK_NAMES}
        STATE["error"] = ""
    finally:
        STATE["last_duration"] = time.monotonic() - started


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
