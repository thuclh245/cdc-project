"""Run fault tolerance scenarios against the local CDC stack.

Examples:
    python -m scripts.validation.fault_tolerance_test --scenario taskmanager
    python -m scripts.validation.fault_tolerance_test --scenario all
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass

from scripts.monitoring.cdc_validation_exporter import load_replication_slots
from scripts.validation.compare_postgres_clickhouse import run_checks
from scripts.validation.verify_stack import verify_jobs_and_slots, verify_logs, verify_tables


@dataclass(frozen=True)
class Scenario:
    key: str
    label: str
    container: str
    check_flink: bool = True
    check_validation: bool = True
    check_metrics: bool = False
    resubmit_if_job_missing: bool = False


@dataclass
class ScenarioResult:
    label: str
    recovery_seconds: float
    ready: bool
    validation: bool | None
    max_slot_lag_bytes: int
    note: str


SCENARIOS = {
    "taskmanager": Scenario(
        key="taskmanager",
        label="Restart TaskManager",
        container="flink-taskmanager",
    ),
    "jobmanager": Scenario(
        key="jobmanager",
        label="Restart JobManager",
        container="flink-jobmanager",
        resubmit_if_job_missing=True,
    ),
    "clickhouse": Scenario(
        key="clickhouse",
        label="Restart ClickHouse",
        container="clickhouse-sink",
    ),
    "exporter": Scenario(
        key="exporter",
        label="Restart validation exporter",
        container="cdc-validation-exporter",
        check_flink=False,
        check_validation=False,
        check_metrics=True,
    ),
}


def run_command(args: list[str], timeout: int = 120, check: bool = True) -> str:
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n{output}"
        )
    return output


def container_state(container: str) -> str:
    return run_command(
        [
            "docker",
            "inspect",
            "--format",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
            container,
        ],
        timeout=15,
    ).strip()


def wait_for_container(container: str, timeout: int) -> str:
    deadline = time.monotonic() + timeout
    last_state = "unknown"
    while time.monotonic() < deadline:
        try:
            last_state = container_state(container)
        except Exception as exc:  # noqa: BLE001 - keep polling while Docker settles.
            last_state = str(exc) or exc.__class__.__name__
        if last_state in {"healthy", "running"}:
            return last_state
        time.sleep(2)
    raise RuntimeError(f"{container} did not become healthy/running: last_state={last_state}")


def max_slot_lag_bytes() -> int:
    slots = load_replication_slots()
    if not slots:
        return 0
    return max(values["lag_bytes"] for values in slots.values())


def wait_for_exporter_metrics(timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen("http://localhost:9108/metrics", timeout=5) as response:
                body = response.read().decode("utf-8", errors="replace")
            if "cdc_validation_up" in body and "cdc_postgres_active_logical_replication_slots" in body:
                return
            last_error = "required metric names were not present"
        except Exception as exc:  # noqa: BLE001 - exporter is expected to be briefly down.
            last_error = str(exc) or exc.__class__.__name__
        time.sleep(2)
    raise RuntimeError(f"exporter metrics did not recover: {last_error}")


def resubmit_job() -> None:
    print("[INFO] No running CDC job after JobManager restart; resubmitting SQL job...")
    run_command(["docker", "start", "-a", "flink-job-submitter"], timeout=180)


def verify_runtime(scenario: Scenario, timeout: int) -> str:
    if not scenario.check_flink:
        if scenario.check_metrics:
            wait_for_exporter_metrics(timeout)
        return "metrics endpoint recovered"

    verify_tables()
    try:
        verify_jobs_and_slots(timeout=timeout)
    except RuntimeError:
        if not scenario.resubmit_if_job_missing:
            raise
        resubmit_job()
        verify_jobs_and_slots(timeout=timeout)
        return "job resubmitted after JobManager restart"

    return "job stayed RUNNING"


def run_scenario(scenario: Scenario, wait_timeout: int) -> ScenarioResult:
    print(f"\n== {scenario.label} ==")
    print(f"[INFO] Restarting {scenario.container}...")
    started = time.monotonic()
    run_command(["docker", "restart", scenario.container], timeout=120)
    state = wait_for_container(scenario.container, wait_timeout)
    note = verify_runtime(scenario, wait_timeout)
    ready_at = time.monotonic()

    validation_ok: bool | None = None
    if scenario.check_validation:
        print("[INFO] Running data validation after recovery...")
        validation_ok = run_checks()

    recovery_seconds = ready_at - started
    lag_bytes = max_slot_lag_bytes()
    ready_ok = state in {"healthy", "running"}
    print(
        f"[RESULT] {scenario.label}: ready={ready_ok}, "
        f"validation={validation_ok}, recovery={recovery_seconds:.1f}s, "
        f"max_slot_lag_bytes={lag_bytes}, note={note}"
    )
    return ScenarioResult(
        label=scenario.label,
        recovery_seconds=recovery_seconds,
        ready=ready_ok,
        validation=validation_ok,
        max_slot_lag_bytes=lag_bytes,
        note=note,
    )


def print_summary(results: list[ScenarioResult]) -> None:
    print("\nSummary")
    print("| Scenario | Recovery time | Ready | Validate | Max slot lag | Note |")
    print("| --- | ---: | --- | --- | ---: | --- |")
    for result in results:
        validate = "SKIP" if result.validation is None else ("PASS" if result.validation else "FAIL")
        print(
            f"| {result.label} | {result.recovery_seconds:.1f}s | "
            f"{'PASS' if result.ready else 'FAIL'} | {validate} | "
            f"{result.max_slot_lag_bytes} | {result.note} |"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=[*SCENARIOS, "all"],
        default="all",
        help="fault tolerance scenario to run",
    )
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=120,
        help="seconds to wait for service and CDC recovery checks",
    )
    parser.add_argument(
        "--skip-baseline",
        action="store_true",
        help="skip baseline readiness checks before restarting services",
    )
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    args = parse_args()
    selected = list(SCENARIOS.values()) if args.scenario == "all" else [SCENARIOS[args.scenario]]

    try:
        if not args.skip_baseline:
            print("[INFO] Running baseline readiness checks...")
            verify_tables()
            verify_jobs_and_slots(timeout=args.wait_timeout)
            verify_logs()

        results = [run_scenario(scenario, args.wait_timeout) for scenario in selected]
    except Exception as exc:  # noqa: BLE001 - CLI should return a readable failure.
        print(f"[FAIL] {str(exc) or exc.__class__.__name__}", file=sys.stderr)
        return 1

    print_summary(results)
    if any(not result.ready or result.validation is False for result in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
