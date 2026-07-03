"""Run Version 3 Flink checkpoint/savepoint recovery checks.

The test is intentionally scoped to Flink recovery behavior:

* prove a completed checkpoint exists in MinIO before restart;
* restart TaskManager and wait for the CDC job/slots/checkpoints to recover;
* optionally restart JobManager and record the standalone-mode behavior;
* optionally trigger a savepoint and list the resulting MinIO object prefix;
* run PostgreSQL-vs-ClickHouse validation after recovery.

Examples:
    python -m scripts.validation.flink_recovery_test
    python -m scripts.validation.flink_recovery_test --include-jobmanager
    python -m scripts.validation.flink_recovery_test --report-file "docs/Version 3/result.md"
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from scripts.monitoring.cdc_validation_exporter import load_replication_slots
from scripts.validation.compare_postgres_clickhouse import run_checks
from scripts.validation.verify_stack import (
    verify_checkpoint_objects,
    verify_jobs_and_slots,
    verify_logs,
    verify_minio,
    verify_seed_data,
    verify_tables,
)


JOB_NAME = "ecommerce-postgres-to-clickhouse-cdc"
MINIO_CHECKPOINT_PREFIX = "checkpoints"
MINIO_SAVEPOINT_PREFIX = "savepoints"


@dataclass
class RuntimeSnapshot:
    job_id: str | None
    job_state: str
    completed_checkpoints: int
    latest_checkpoint_path: str
    max_slot_lag_bytes: int


@dataclass
class StepResult:
    name: str
    status: str
    seconds: float
    note: str


@dataclass
class RecoveryReport:
    started_at: str
    baseline: RuntimeSnapshot
    taskmanager: StepResult
    taskmanager_validation: bool
    jobmanager: StepResult | None
    jobmanager_validation: bool | None
    savepoint_status: str
    savepoint_path: str
    savepoint_note: str
    final_job_id: str | None


def run_command(args: list[str], timeout: int = 120, check: bool = True) -> str:
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n{output}"
        )
    return output


def get_json(path: str, timeout: int = 5) -> dict:
    with urllib.request.urlopen(f"http://localhost:8081{path}", timeout=timeout) as response:
        return json.load(response)


def max_slot_lag_bytes() -> int:
    slots = load_replication_slots()
    if not slots:
        return 0
    return max(values["lag_bytes"] for values in slots.values())


def jobs() -> list[dict]:
    return get_json("/jobs/overview").get("jobs", [])


def running_job_id() -> str | None:
    running = [
        job
        for job in jobs()
        if job.get("name") == JOB_NAME and job.get("state") == "RUNNING"
    ]
    if not running:
        return None
    return running[0].get("jid")


def job_state(job_id: str | None) -> str:
    if job_id is None:
        states = [f"{job.get('name')}={job.get('state')}" for job in jobs()]
        return ", ".join(states) if states else "MISSING"
    for job in jobs():
        if job.get("jid") == job_id:
            return str(job.get("state", "UNKNOWN"))
    return "MISSING"


def checkpoint_overview(job_id: str) -> dict:
    return get_json(f"/jobs/{job_id}/checkpoints")


def completed_checkpoint_count(job_id: str | None) -> int:
    if job_id is None:
        return 0
    return int(checkpoint_overview(job_id).get("counts", {}).get("completed", 0))


def latest_completed_checkpoint_path(job_id: str | None) -> str:
    if job_id is None:
        return ""
    latest = checkpoint_overview(job_id).get("latest", {})
    completed = latest.get("completed") or {}
    return str(completed.get("external_path") or "")


def snapshot(job_id: str | None) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        job_id=job_id,
        job_state=job_state(job_id),
        completed_checkpoints=completed_checkpoint_count(job_id),
        latest_checkpoint_path=latest_completed_checkpoint_path(job_id),
        max_slot_lag_bytes=max_slot_lag_bytes(),
    )


def wait_for_container(container: str, timeout: int) -> str:
    deadline = time.monotonic() + timeout
    last_state = "unknown"
    while time.monotonic() < deadline:
        try:
            last_state = run_command(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                    container,
                ],
                timeout=15,
            ).strip()
        except Exception as exc:  # noqa: BLE001 - Docker state is transient during restart.
            last_state = str(exc) or exc.__class__.__name__
        if last_state in {"healthy", "running"}:
            return last_state
        time.sleep(2)
    raise RuntimeError(f"{container} did not become healthy/running: last_state={last_state}")


def wait_for_new_checkpoint(job_id: str, initial_completed: int, timeout: int) -> int:
    deadline = time.monotonic() + timeout
    last_completed = initial_completed
    while time.monotonic() < deadline:
        last_completed = completed_checkpoint_count(job_id)
        if last_completed > initial_completed:
            return last_completed
        time.sleep(5)
    raise RuntimeError(
        f"no new completed checkpoint within {timeout}s "
        f"(initial={initial_completed}, last={last_completed})"
    )


def baseline_snapshot(wait_timeout: int) -> RuntimeSnapshot:
    verify_tables()
    verify_minio()
    job_id = verify_jobs_and_slots(timeout=wait_timeout)
    verify_seed_data()
    verify_logs()
    completed = completed_checkpoint_count(job_id)
    if completed < 1:
        raise RuntimeError(f"expected at least one completed checkpoint, got {completed}")
    verify_checkpoint_objects()
    snap = snapshot(job_id)
    print(
        "[OK] baseline checkpoint: "
        f"completed={snap.completed_checkpoints}, path={snap.latest_checkpoint_path or 'n/a'}"
    )
    return snap


def restart_taskmanager(baseline: RuntimeSnapshot, wait_timeout: int) -> StepResult:
    print("\n== Restart TaskManager ==")
    started = time.monotonic()
    run_command(["docker", "restart", "flink-taskmanager"], timeout=120)
    wait_for_container("flink-taskmanager", wait_timeout)
    job_id = verify_jobs_and_slots(timeout=wait_timeout)
    initial = completed_checkpoint_count(job_id)
    completed = wait_for_new_checkpoint(job_id, initial, wait_timeout)
    seconds = time.monotonic() - started
    same_job = job_id == baseline.job_id
    note = (
        f"job RUNNING, checkpoint {initial}->{completed}, "
        f"job_id_same={'yes' if same_job else 'no'}"
    )
    print(f"[RESULT] TaskManager recovery PASS in {seconds:.1f}s: {note}")
    return StepResult("TaskManager restart", "PASS", seconds, note)


def start_submitter(timeout: int) -> str:
    return run_command(["docker", "start", "-a", "flink-job-submitter"], timeout=timeout)


def restart_jobmanager(wait_timeout: int, resubmit: bool) -> tuple[StepResult, bool | None]:
    print("\n== Restart JobManager ==")
    started = time.monotonic()
    run_command(["docker", "restart", "flink-jobmanager"], timeout=120)
    wait_for_container("flink-jobmanager", wait_timeout)

    deadline = time.monotonic() + wait_timeout
    recovered_job_id: str | None = None
    while time.monotonic() < deadline:
        recovered_job_id = running_job_id()
        if recovered_job_id:
            break
        time.sleep(5)

    if recovered_job_id:
        completed = completed_checkpoint_count(recovered_job_id)
        wait_for_new_checkpoint(recovered_job_id, completed, wait_timeout)
        seconds = time.monotonic() - started
        note = "job recovered automatically after JobManager restart"
        print(f"[RESULT] JobManager recovery PASS in {seconds:.1f}s: {note}")
        return StepResult("JobManager restart", "PASS", seconds, note), None

    if not resubmit:
        seconds = time.monotonic() - started
        note = "standalone JobManager restart lost the running job; resubmit/restore required"
        print(f"[RESULT] JobManager recovery OBSERVED in {seconds:.1f}s: {note}")
        return StepResult("JobManager restart", "OBSERVED", seconds, note), None

    print("[INFO] No running CDC job after JobManager restart; resubmitting SQL job...")
    start_submitter(timeout=max(180, wait_timeout))
    job_id = verify_jobs_and_slots(timeout=wait_timeout)
    completed = completed_checkpoint_count(job_id)
    wait_for_new_checkpoint(job_id, completed, wait_timeout)
    validation_ok = run_checks()
    seconds = time.monotonic() - started
    note = (
        "standalone JobManager did not retain the job; SQL job was resubmitted "
        "and checkpointing resumed"
    )
    print(f"[RESULT] JobManager recovery RESUBMITTED in {seconds:.1f}s: {note}")
    return StepResult("JobManager restart", "RESUBMITTED", seconds, note), validation_ok


def trigger_savepoint(job_id: str, timeout: int) -> tuple[str, str, str]:
    print("\n== Trigger Savepoint ==")
    output = run_command(
        [
            "docker",
            "exec",
            "flink-jobmanager",
            "/opt/flink/bin/flink",
            "savepoint",
            job_id,
            f"s3://flink-state/{MINIO_SAVEPOINT_PREFIX}",
        ],
        timeout=timeout,
    )
    path = parse_savepoint_path(output)
    if not path:
        raise RuntimeError(f"savepoint command did not return a path:\n{output}")

    list_savepoints()
    note = "savepoint created; restore is documented as a manual standalone step"
    print(f"[RESULT] Savepoint PASS: {path}")
    return "PASS", path, note


def parse_savepoint_path(output: str) -> str:
    match = re.search(r"(s3://\S+)", output)
    return match.group(1).rstrip(".") if match else ""


def list_savepoints() -> str:
    return run_command(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-c",
            (
                'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" '
                '"$MINIO_ROOT_PASSWORD" >/dev/null && '
                f'mc ls --recursive "local/$MINIO_FLINK_BUCKET/{MINIO_SAVEPOINT_PREFIX}" | tail -20'
            ),
        ],
        timeout=120,
    )


def write_report(report: RecoveryReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_report(report), encoding="utf-8")
    print(f"[OK] wrote report: {path}")


def status_text(value: bool | None) -> str:
    if value is None:
        return "SKIP"
    return "PASS" if value else "FAIL"


def render_report(report: RecoveryReport) -> str:
    jobmanager_rows = ""
    if report.jobmanager is not None:
        jobmanager_rows = (
            f"| JobManager restart | {report.jobmanager.status} | "
            f"{report.jobmanager.seconds:.1f}s | "
            f"{status_text(report.jobmanager_validation)} | {report.jobmanager.note} |\n"
        )

    return f"""# Phase 2 result - Flink recovery test

## Tóm tắt

Phase 2 đã kiểm tra recovery của Flink CDC job từ checkpoint/savepoint trên MinIO.

- Thời điểm chạy: `{report.started_at}`
- Baseline job ID: `{report.baseline.job_id or 'n/a'}`
- Final job ID: `{report.final_job_id or 'n/a'}`
- Baseline completed checkpoints: `{report.baseline.completed_checkpoints}`
- Latest checkpoint path: `{report.baseline.latest_checkpoint_path or 'n/a'}`
- Savepoint status: `{report.savepoint_status}`

## Kết quả

| Hạng mục | Trạng thái | Recovery time | Validation | Ghi chú |
| --- | --- | ---: | --- | --- |
| TaskManager restart | {report.taskmanager.status} | {report.taskmanager.seconds:.1f}s | {status_text(report.taskmanager_validation)} | {report.taskmanager.note} |
{jobmanager_rows}| Savepoint | {report.savepoint_status} | n/a | n/a | {report.savepoint_note} |

## Runtime snapshot

| Metric | Giá trị |
| --- | --- |
| Job state trước test | `{report.baseline.job_state}` |
| Max replication slot lag trước test | `{report.baseline.max_slot_lag_bytes}` bytes |
| Savepoint path | `{report.savepoint_path or 'n/a'}` |

## Savepoint/restore command

Tạo savepoint:

```bash
docker exec flink-jobmanager /opt/flink/bin/flink savepoint <job_id> s3://flink-state/savepoints
```

Trong standalone Docker Compose hiện tại, JobManager restart không có Flink HA metadata store riêng. Nếu job mất sau JobManager restart, cần resubmit SQL job hoặc nâng cấp sang Flink HA trước khi coi đây là auto recovery của JobManager.

Kiểm tra restore cần được thực hiện có kiểm soát với savepoint path cụ thể:

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list -a
# Nếu không còn job RUNNING, submit lại job với cấu hình restore từ savepoint tương ứng.
```

## Acceptance criteria

| Tiêu chí | Trạng thái |
| --- | --- |
| Có completed checkpoint trước restart | PASS |
| Restart TaskManager xong job RUNNING | {report.taskmanager.status} |
| Validation pass sau TaskManager recovery | {status_text(report.taskmanager_validation)} |
| Có tài liệu recovery time | PASS |
| Savepoint command/result rõ ràng | {report.savepoint_status} |
| Không fake restore pass | PASS |
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=180,
        help="seconds to wait for Flink/container/checkpoint recovery",
    )
    parser.add_argument(
        "--include-jobmanager",
        action="store_true",
        help="also restart JobManager and record standalone-mode behavior",
    )
    parser.add_argument(
        "--resubmit-after-jobmanager",
        action="store_true",
        help="resubmit the SQL job if standalone JobManager restart loses it",
    )
    parser.add_argument(
        "--skip-savepoint",
        action="store_true",
        help="skip savepoint creation",
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        help="write a Markdown result report to this path",
    )
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    args = parse_args()
    started_at = datetime.now().isoformat(timespec="seconds")

    try:
        baseline = baseline_snapshot(args.wait_timeout)
        taskmanager = restart_taskmanager(baseline, args.wait_timeout)
        print("[INFO] Running validation after TaskManager recovery...")
        taskmanager_validation = run_checks()

        jobmanager: StepResult | None = None
        jobmanager_validation: bool | None = None
        if args.include_jobmanager:
            jobmanager, jobmanager_validation = restart_jobmanager(
                args.wait_timeout,
                args.resubmit_after_jobmanager,
            )

        savepoint_status = "SKIP"
        savepoint_path = ""
        savepoint_note = "savepoint creation skipped"
        current_job_id = running_job_id()
        if not args.skip_savepoint:
            if current_job_id is None:
                savepoint_status = "SKIP"
                savepoint_note = "no RUNNING job available for savepoint"
            else:
                savepoint_status, savepoint_path, savepoint_note = trigger_savepoint(
                    current_job_id,
                    timeout=max(240, args.wait_timeout),
                )

        final_job_id = running_job_id()
        report = RecoveryReport(
            started_at=started_at,
            baseline=baseline,
            taskmanager=taskmanager,
            taskmanager_validation=taskmanager_validation,
            jobmanager=jobmanager,
            jobmanager_validation=jobmanager_validation,
            savepoint_status=savepoint_status,
            savepoint_path=savepoint_path,
            savepoint_note=savepoint_note,
            final_job_id=final_job_id,
        )
        if args.report_file:
            write_report(report, args.report_file)
    except Exception as exc:  # noqa: BLE001 - CLI should report readable failures.
        print(f"[FAIL] {str(exc) or exc.__class__.__name__}", file=sys.stderr)
        return 1

    failures = [not taskmanager_validation]
    if jobmanager_validation is not None:
        failures.append(not jobmanager_validation)
    if savepoint_status == "FAIL":
        failures.append(True)
    return 1 if any(failures) else 0


if __name__ == "__main__":
    raise SystemExit(main())
