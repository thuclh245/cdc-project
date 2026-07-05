"""Deep CDC latency benchmark with repeated runs and load scenarios.

The existing cdc_latency_benchmark.py is intentionally small and sequential.
This script is for evidence collection: it inserts many probe rows
asynchronously, optionally runs the application stress stream in parallel, and
records PostgreSQL slot lag plus Flink checkpoint/backpressure snapshots.

Example:
    python -m scripts.benchmark.deep_cdc_latency_benchmark \
        --scenario baseline:0:300:0.10 \
        --scenario app-1000:1000:300:0.05 \
        --repeats 3 \
        --report-file "docs/Version 4/09_DEEP_LATENCY_RESULT_2026-07-05.md"
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import socket
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.database.connection import get_conn
from scripts.benchmark import stress_stream


MetricSamples = list[float]


@dataclass(frozen=True)
class Scenario:
    name: str
    load_rate_per_min: int
    samples: int
    probe_interval_s: float


@dataclass
class ProbeResult:
    probe_id: int
    committed_monotonic: float
    commit_latency_s: float
    source_to_sink_s: float | None = None
    client_observed_s: float | None = None
    timed_out: bool = False


@dataclass
class RunResult:
    scenario: Scenario
    repeat: int
    run_key: str
    started_at: str
    duration_s: float
    requested_samples: int
    observed_samples: int
    timeouts: int
    commit_latencies: MetricSamples
    source_to_sink_latencies: MetricSamples
    client_observed_latencies: MetricSamples
    slot_lag_max_bytes: int
    probe_slot_lag_max_bytes: int
    slot_lag_final_bytes: int
    probe_slot_lag_final_bytes: int
    backpressured_ms_per_s_max: float
    soft_backpressured_ms_per_s_max: float
    busy_ms_per_s_max: float
    is_backpressured_max: float
    checkpoint_before: dict[str, Any] = field(default_factory=dict)
    checkpoint_after: dict[str, Any] = field(default_factory=dict)
    stress_actual_rate_per_min: float | None = None
    stress_measured_events: int | None = None
    stress_errors: int | None = None


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


def get_json(url: str, timeout: float = 5) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception:
        return None


def active_flink_job_id() -> str | None:
    payload = get_json("http://localhost:8081/jobs/overview")
    if not payload:
        return None
    for job in payload.get("jobs", []):
        if job.get("state") == "RUNNING":
            return str(job.get("jid"))
    return None


def checkpoint_snapshot() -> dict[str, Any]:
    jid = active_flink_job_id()
    if not jid:
        return {}
    payload = get_json(f"http://localhost:8081/jobs/{jid}/checkpoints") or {}
    return {
        "job_id": jid,
        "counts": payload.get("counts", {}),
        "summary": payload.get("summary", {}),
    }


def scrape_prometheus_metrics(port: int) -> str:
    try:
        with urlopen(f"http://localhost:{port}/metrics", timeout=5) as response:
            return response.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def max_metric_value(metrics_text: str, metric_name: str) -> float:
    max_value = 0.0
    prefix = metric_name + "{"
    for line in metrics_text.splitlines():
        if not line.startswith(prefix):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        try:
            max_value = max(max_value, float(parts[1]))
        except ValueError:
            continue
    return max_value


def all_slot_lags() -> dict[str, int]:
    sql = """
        SELECT
            slot_name,
            COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::BIGINT
        FROM pg_replication_slots
        WHERE slot_type = 'logical'
    """
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return {str(name): int(lag) for name, lag in cursor.fetchall()}
    finally:
        conn.close()


class MetricsSampler:
    def __init__(self, interval_s: float = 2.0) -> None:
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.slot_lag_max_bytes = 0
        self.probe_slot_lag_max_bytes = 0
        self.slot_lag_final_bytes = 0
        self.probe_slot_lag_final_bytes = 0
        self.backpressured_ms_per_s_max = 0.0
        self.soft_backpressured_ms_per_s_max = 0.0
        self.busy_ms_per_s_max = 0.0
        self.is_backpressured_max = 0.0

    def start(self) -> None:
        self._sample_once()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval_s + 2)
        self._sample_once()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._sample_once()

    def _sample_once(self) -> None:
        try:
            lags = all_slot_lags()
            self.slot_lag_final_bytes = max(lags.values(), default=0)
            self.probe_slot_lag_final_bytes = lags.get("flink_cdc_latency_probe_slot", 0)
            self.slot_lag_max_bytes = max(self.slot_lag_max_bytes, self.slot_lag_final_bytes)
            self.probe_slot_lag_max_bytes = max(
                self.probe_slot_lag_max_bytes,
                self.probe_slot_lag_final_bytes,
            )
        except Exception:
            pass

        metrics = scrape_prometheus_metrics(9250)
        if metrics:
            self.backpressured_ms_per_s_max = max(
                self.backpressured_ms_per_s_max,
                max_metric_value(
                    metrics,
                    "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
                ),
            )
            self.soft_backpressured_ms_per_s_max = max(
                self.soft_backpressured_ms_per_s_max,
                max_metric_value(
                    metrics,
                    "flink_taskmanager_job_task_softBackPressuredTimeMsPerSecond",
                ),
            )
            self.busy_ms_per_s_max = max(
                self.busy_ms_per_s_max,
                max_metric_value(metrics, "flink_taskmanager_job_task_busyTimeMsPerSecond"),
            )
            self.is_backpressured_max = max(
                self.is_backpressured_max,
                max_metric_value(metrics, "flink_taskmanager_job_task_isBackPressured"),
            )


class LoadWorker:
    def __init__(self, rate_per_min: int, warmup_s: int, run_key: str) -> None:
        self.rate_per_min = rate_per_min
        self.warmup_s = warmup_s
        self.run_key = run_key
        self.total_events = 0
        self.measured_events = 0
        self.errors = 0
        self.actual_rate_per_min = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        if self.rate_per_min <= 0:
            return
        self._thread.start()

    def stop(self) -> None:
        if self.rate_per_min <= 0:
            return
        self._stop.set()
        self._thread.join(timeout=10)

    def _run(self) -> None:
        rate_per_sec = max(0.001, self.rate_per_min / 60.0)
        bucket = stress_stream.TokenBucket(rate_per_sec)
        start = time.monotonic()
        warmup_end = start + self.warmup_s
        measured_start = None
        measured_end = None
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        seq = 0
        try:
            while not self._stop.is_set():
                bucket.consume(1)
                op = random.choices(
                    stress_stream.OPS,
                    weights=stress_stream.OP_WEIGHTS,
                    k=1,
                )[0]
                try:
                    stress_stream.do_one_event(conn, cur, self.run_key, seq, op)
                    seq += 1
                    self.total_events += 1
                    now = time.monotonic()
                    if now >= warmup_end:
                        if measured_start is None:
                            measured_start = now
                        measured_end = now
                        self.measured_events += 1
                except Exception:
                    self.errors += 1
                    conn.rollback()
                    time.sleep(0.2)
        finally:
            cur.close()
            conn.close()
            if measured_start is not None and measured_end is not None:
                duration = max(1.0, measured_end - measured_start)
                self.actual_rate_per_min = (self.measured_events / duration) * 60


def insert_probe(conn, probe_key: str, payload: str) -> ProbeResult:
    started = time.monotonic()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO cdc_latency_probe (probe_key, payload, source_updated_at)
            VALUES (%s, %s, clock_timestamp())
            RETURNING probe_id
            """,
            (probe_key, payload),
        )
        probe_id = int(cursor.fetchone()[0])
    conn.commit()
    committed = time.monotonic()
    return ProbeResult(
        probe_id=probe_id,
        committed_monotonic=committed,
        commit_latency_s=committed - started,
    )


def chunked(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def poll_probe_results(
    clickhouse: ClickHouseClient,
    pending: dict[int, ProbeResult],
    now_monotonic: float,
) -> None:
    if not pending:
        return
    for probe_ids in chunked(list(pending), 500):
        id_list = ",".join(str(probe_id) for probe_id in probe_ids)
        rows = clickhouse.query(
            f"""
            SELECT
                probe_id,
                greatest(
                    0,
                    toUnixTimestamp64Micro(sink_observed_at)
                    - toUnixTimestamp64Micro(source_updated_at)
                ) / 1000000
            FROM ecommerce_ods.cdc_latency_probe_sink FINAL
            WHERE probe_id IN ({id_list})
              AND deleted_at IS NULL
            """
        )
        if not rows:
            continue
        for row in rows.splitlines():
            fields = row.split("\t")
            if len(fields) != 2:
                continue
            probe_id = int(fields[0])
            result = pending.pop(probe_id, None)
            if result is None:
                continue
            result.source_to_sink_s = float(fields[1])
            result.client_observed_s = now_monotonic - result.committed_monotonic


def run_probe_batch(
    scenario: Scenario,
    repeat: int,
    timeout_s: float,
    poll_interval_s: float,
    sampler: MetricsSampler,
) -> tuple[list[ProbeResult], float]:
    clickhouse = ClickHouseClient()
    probe_key = (
        f"deep-cdc-{scenario.name}-r{repeat}-"
        f"{socket.gethostname()}-{int(time.time())}"
    )
    conn = get_conn()
    conn.autocommit = False
    results: list[ProbeResult] = []
    pending: dict[int, ProbeResult] = {}
    next_insert_at = time.monotonic()
    started = time.monotonic()
    deadline_by_probe: dict[int, float] = {}

    try:
        while len(results) < scenario.samples or pending:
            now = time.monotonic()
            while len(results) < scenario.samples and now >= next_insert_at:
                index = len(results) + 1
                payload = f"{probe_key}:{index}:{time.time_ns()}"
                result = insert_probe(conn, probe_key, payload)
                results.append(result)
                pending[result.probe_id] = result
                deadline_by_probe[result.probe_id] = result.committed_monotonic + timeout_s
                next_insert_at += scenario.probe_interval_s
                now = time.monotonic()

            poll_probe_results(clickhouse, pending, now)

            expired = [
                probe_id
                for probe_id, deadline in deadline_by_probe.items()
                if probe_id in pending and now >= deadline
            ]
            for probe_id in expired:
                pending[probe_id].timed_out = True
                pending.pop(probe_id, None)

            if pending or len(results) < scenario.samples:
                time.sleep(poll_interval_s)
    finally:
        conn.close()
        sampler.stop()

    return results, time.monotonic() - started


def run_one_scenario(
    scenario: Scenario,
    repeat: int,
    load_warmup_s: int,
    load_cooldown_s: int,
    timeout_s: float,
    poll_interval_s: float,
) -> RunResult:
    run_key = (
        f"deep-{scenario.name}-r{repeat}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    )
    checkpoint_before = checkpoint_snapshot()
    sampler = MetricsSampler()
    load_worker = LoadWorker(scenario.load_rate_per_min, load_warmup_s, run_key)
    load_worker.start()

    if scenario.load_rate_per_min > 0 and load_warmup_s > 0:
        time.sleep(load_warmup_s)

    started_at = datetime.now(timezone.utc).isoformat()
    sampler.start()
    probe_results, duration_s = run_probe_batch(
        scenario,
        repeat,
        timeout_s,
        poll_interval_s,
        sampler,
    )

    if scenario.load_rate_per_min > 0 and load_cooldown_s > 0:
        time.sleep(load_cooldown_s)
    load_worker.stop()
    checkpoint_after = checkpoint_snapshot()

    observed = [result for result in probe_results if result.source_to_sink_s is not None]
    return RunResult(
        scenario=scenario,
        repeat=repeat,
        run_key=run_key,
        started_at=started_at,
        duration_s=duration_s,
        requested_samples=scenario.samples,
        observed_samples=len(observed),
        timeouts=sum(1 for result in probe_results if result.timed_out),
        commit_latencies=[result.commit_latency_s for result in probe_results],
        source_to_sink_latencies=[
            float(result.source_to_sink_s)
            for result in observed
            if result.source_to_sink_s is not None
        ],
        client_observed_latencies=[
            float(result.client_observed_s)
            for result in observed
            if result.client_observed_s is not None
        ],
        slot_lag_max_bytes=sampler.slot_lag_max_bytes,
        probe_slot_lag_max_bytes=sampler.probe_slot_lag_max_bytes,
        slot_lag_final_bytes=sampler.slot_lag_final_bytes,
        probe_slot_lag_final_bytes=sampler.probe_slot_lag_final_bytes,
        backpressured_ms_per_s_max=sampler.backpressured_ms_per_s_max,
        soft_backpressured_ms_per_s_max=sampler.soft_backpressured_ms_per_s_max,
        busy_ms_per_s_max=sampler.busy_ms_per_s_max,
        is_backpressured_max=sampler.is_backpressured_max,
        checkpoint_before=checkpoint_before,
        checkpoint_after=checkpoint_after,
        stress_actual_rate_per_min=load_worker.actual_rate_per_min
        if scenario.load_rate_per_min > 0
        else None,
        stress_measured_events=load_worker.measured_events
        if scenario.load_rate_per_min > 0
        else None,
        stress_errors=load_worker.errors if scenario.load_rate_per_min > 0 else None,
    )


def percentile(samples: MetricSamples, percent: int) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, math.ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


def mean_ci95(samples: MetricSamples) -> tuple[float, float, float]:
    if not samples:
        return 0.0, 0.0, 0.0
    mean = statistics.fmean(samples)
    if len(samples) < 2:
        return mean, mean, mean
    stdev = statistics.stdev(samples)
    half_width = 1.96 * stdev / math.sqrt(len(samples))
    return mean, mean - half_width, mean + half_width


def fmt_seconds(value: float) -> str:
    return f"{value:.3f}s"


def fmt_ms(value: float) -> str:
    return f"{value * 1000:.1f} ms"


def fmt_bytes(value: int) -> str:
    current = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if current < 1024 or unit == "GB":
            return f"{current:.1f} {unit}" if unit != "B" else f"{int(current)} B"
        current /= 1024
    return f"{value} B"


def checkpoint_delta(before: dict[str, Any], after: dict[str, Any], key: str) -> int:
    before_counts = before.get("counts", {}) if before else {}
    after_counts = after.get("counts", {}) if after else {}
    try:
        return int(after_counts.get(key, 0)) - int(before_counts.get(key, 0))
    except Exception:
        return 0


def metric_summary(samples: MetricSamples) -> dict[str, float]:
    mean, ci_low, ci_high = mean_ci95(samples)
    return {
        "mean": mean,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p50": percentile(samples, 50),
        "p95": percentile(samples, 95),
        "p99": percentile(samples, 99),
        "max": max(samples) if samples else 0.0,
    }


def write_report(path: Path, results: list[RunResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines: list[str] = [
        "# Deep CDC Latency Benchmark Result",
        "",
        f"Generated at: {generated_at}",
        "",
        "## Method",
        "",
        "- Probe rows are inserted into PostgreSQL table `cdc_latency_probe`.",
        "- PostgreSQL/WAL write cost is approximated by client-side insert+commit latency.",
        "- End-to-end CDC latency is measured by `sink_observed_at - source_updated_at` in ClickHouse.",
        "- Flink/backpressure and PostgreSQL WAL slot lag are sampled during each repeat.",
        "- The current SQL job does not emit timestamps between WAL read, Flink operators, and ClickHouse sink, so those internal stages are inferred from proxy metrics rather than directly timestamped.",
        "",
        "## Scenario Summary",
        "",
        "| Scenario | Load target | Actual load | Samples | Timeouts | Source->sink mean 95% CI | p50 | p95 | p99 | Max | Commit p95 | Max probe slot lag | Max all slot lag | Backpressure max | Failed checkpoints delta |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    by_scenario: dict[str, list[RunResult]] = {}
    for result in results:
        by_scenario.setdefault(result.scenario.name, []).append(result)

    for scenario_name, scenario_results in by_scenario.items():
        source_samples = [
            value
            for result in scenario_results
            for value in result.source_to_sink_latencies
        ]
        commit_samples = [
            value
            for result in scenario_results
            for value in result.commit_latencies
        ]
        summary = metric_summary(source_samples)
        commit_summary = metric_summary(commit_samples)
        actual_rates = [
            result.stress_actual_rate_per_min
            for result in scenario_results
            if result.stress_actual_rate_per_min is not None
        ]
        actual_rate = statistics.fmean(actual_rates) if actual_rates else 0.0
        failed_delta = sum(
            checkpoint_delta(result.checkpoint_before, result.checkpoint_after, "failed")
            for result in scenario_results
        )
        requested = sum(result.requested_samples for result in scenario_results)
        observed = sum(result.observed_samples for result in scenario_results)
        lines.append(
            "| "
            + " | ".join(
                [
                    scenario_name,
                    f"{scenario_results[0].scenario.load_rate_per_min}/min",
                    f"{actual_rate:.1f}/min" if actual_rates else "n/a",
                    f"{observed}/{requested}",
                    str(sum(result.timeouts for result in scenario_results)),
                    f"{fmt_seconds(summary['mean'])} [{fmt_seconds(summary['ci_low'])}, {fmt_seconds(summary['ci_high'])}]",
                    fmt_seconds(summary["p50"]),
                    fmt_seconds(summary["p95"]),
                    fmt_seconds(summary["p99"]),
                    fmt_seconds(summary["max"]),
                    fmt_ms(commit_summary["p95"]),
                    fmt_bytes(max(result.probe_slot_lag_max_bytes for result in scenario_results)),
                    fmt_bytes(max(result.slot_lag_max_bytes for result in scenario_results)),
                    f"{max(result.backpressured_ms_per_s_max for result in scenario_results):.1f} ms/s",
                    str(failed_delta),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Repeat Details",
            "",
            "| Scenario | Repeat | Duration | Samples | Source->sink p50 | p95 | p99 | Max | Client-observed p95 | Commit p95 | Probe slot lag max/final | All slot lag max/final | Backpressure max | Busy max | Checkpoints completed/failed delta | Stress events/errors |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for result in results:
        source_summary = metric_summary(result.source_to_sink_latencies)
        client_summary = metric_summary(result.client_observed_latencies)
        commit_summary = metric_summary(result.commit_latencies)
        completed_delta = checkpoint_delta(
            result.checkpoint_before,
            result.checkpoint_after,
            "completed",
        )
        failed_delta = checkpoint_delta(
            result.checkpoint_before,
            result.checkpoint_after,
            "failed",
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    result.scenario.name,
                    str(result.repeat),
                    fmt_seconds(result.duration_s),
                    f"{result.observed_samples}/{result.requested_samples}",
                    fmt_seconds(source_summary["p50"]),
                    fmt_seconds(source_summary["p95"]),
                    fmt_seconds(source_summary["p99"]),
                    fmt_seconds(source_summary["max"]),
                    fmt_seconds(client_summary["p95"]),
                    fmt_ms(commit_summary["p95"]),
                    f"{fmt_bytes(result.probe_slot_lag_max_bytes)} / {fmt_bytes(result.probe_slot_lag_final_bytes)}",
                    f"{fmt_bytes(result.slot_lag_max_bytes)} / {fmt_bytes(result.slot_lag_final_bytes)}",
                    f"{result.backpressured_ms_per_s_max:.1f} ms/s",
                    f"{result.busy_ms_per_s_max:.1f} ms/s",
                    f"{completed_delta}/{failed_delta}",
                    (
                        f"{result.stress_measured_events or 0}/{result.stress_errors or 0}"
                        if result.stress_measured_events is not None
                        else "n/a"
                    ),
                ]
            )
            + " |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_scenario(raw: str) -> Scenario:
    parts = raw.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "scenario must be name:load_rate_per_min:samples:probe_interval_s"
        )
    name, load_rate, samples, interval = parts
    return Scenario(
        name=name,
        load_rate_per_min=int(load_rate),
        samples=int(samples),
        probe_interval_s=float(interval),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deep CDC latency benchmark with repeated probe runs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        type=parse_scenario,
        action="append",
        default=[],
        help="Scenario as name:load_rate_per_min:samples:probe_interval_s",
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--load-warmup", type=int, default=10)
    parser.add_argument("--load-cooldown", type=int, default=5)
    parser.add_argument(
        "--report-file",
        type=Path,
        default=Path("docs/Version 4/09_DEEP_LATENCY_RESULT_2026-07-05.md"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenarios = args.scenario or [
        Scenario("baseline", 0, 300, 0.10),
        Scenario("app-1000", 1000, 300, 0.05),
        Scenario("probe-burst", 0, 1000, 0.005),
    ]
    if args.repeats < 1:
        print("[ERROR] --repeats must be at least 1", file=sys.stderr)
        return 2
    for scenario in scenarios:
        if scenario.samples < 1:
            print(f"[ERROR] scenario {scenario.name}: samples must be at least 1", file=sys.stderr)
            return 2
        if scenario.probe_interval_s < 0:
            print(
                f"[ERROR] scenario {scenario.name}: probe interval must be non-negative",
                file=sys.stderr,
            )
            return 2

    results: list[RunResult] = []
    print("Deep CDC latency benchmark")
    print(f"repeats={args.repeats} timeout={args.timeout:g}s report={args.report_file}")
    for scenario in scenarios:
        print(
            f"\nScenario {scenario.name}: load={scenario.load_rate_per_min}/min "
            f"samples={scenario.samples} interval={scenario.probe_interval_s:g}s"
        )
        for repeat in range(1, args.repeats + 1):
            print(f"  repeat {repeat}/{args.repeats}...", flush=True)
            result = run_one_scenario(
                scenario=scenario,
                repeat=repeat,
                load_warmup_s=args.load_warmup,
                load_cooldown_s=args.load_cooldown,
                timeout_s=args.timeout,
                poll_interval_s=args.poll_interval,
            )
            results.append(result)
            summary = metric_summary(result.source_to_sink_latencies)
            print(
                "    "
                f"observed={result.observed_samples}/{result.requested_samples} "
                f"timeouts={result.timeouts} "
                f"p95={fmt_seconds(summary['p95'])} "
                f"p99={fmt_seconds(summary['p99'])} "
                f"probe_lag_max={fmt_bytes(result.probe_slot_lag_max_bytes)} "
                f"backpressure={result.backpressured_ms_per_s_max:.1f}ms/s",
                flush=True,
            )

    write_report(args.report_file, results)
    print(f"\nReport written to: {args.report_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
