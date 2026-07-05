"""True throughput benchmark with multi-threaded batch writers.

Run from the project root:
    python -m scripts.benchmark.true_throughput_stream \
        --rate 1000 --duration 600 --warmup 60 --workers 4 --batch-size 10
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import socket
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

from scripts.benchmark import stress_stream
from scripts.database.connection import get_conn
from scripts.validation import compare_postgres_clickhouse


MetricSamples = list[float]


@dataclass
class ProbeResult:
    probe_id: int
    committed_monotonic: float
    commit_latency_s: float
    source_to_sink_s: float | None = None
    client_observed_s: float | None = None
    timed_out: bool = False


@dataclass
class WorkerStats:
    worker_id: int
    total_events: int = 0
    measured_events: int = 0
    batches: int = 0
    measured_batches: int = 0
    errors: int = 0
    retries: int = 0
    commit_latencies_s: MetricSamples = field(default_factory=list)


@dataclass
class RunResult:
    run_id: str
    started_at: str
    finished_at: str
    rate_per_min: int
    duration_s: int
    warmup_s: int
    workers: int
    batch_size: int
    total_events: int
    measured_events: int
    actual_rate_per_min: float
    writer_errors: int
    retry_count: int
    commit_latencies_s: MetricSamples
    probe_requested: int
    probe_observed: int
    probe_timeouts: int
    probe_commit_latencies_s: MetricSamples
    source_to_sink_latencies_s: MetricSamples
    client_observed_latencies_s: MetricSamples
    max_slot_lag_bytes: int
    final_slot_lag_bytes: int
    catchup_duration_s: float | None
    active_slots: list[dict[str, Any]]
    checkpoint_before: dict[str, Any]
    checkpoint_after: dict[str, Any]
    backpressured_ms_per_s_max: float
    soft_backpressured_ms_per_s_max: float
    busy_ms_per_s_max: float
    is_backpressured_max: float
    sink_row_growth: dict[str, int]
    clickhouse_parts: dict[str, int]
    active_merges: int | None
    validation_summary: str
    conclusion: str


class ClickHouseClient:
    def __init__(self) -> None:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = os.getenv("CLICKHOUSE_PORT", "8123")
        database = os.getenv("CLICKHOUSE_DB", "ecommerce_ods")
        self.url = f"http://{host}:{port}/?{urlencode({'database': database})}"
        self.user = os.getenv("CLICKHOUSE_USER", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.timeout = float(os.getenv("BENCHMARK_CLICKHOUSE_TIMEOUT", "15"))

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


class GlobalBatchRateLimiter:
    """Thread-safe token bucket shared by all writer workers."""

    def __init__(self, rate_per_second: float) -> None:
        self._rate = max(0.001, rate_per_second)
        self._capacity = max(1.0, rate_per_second)
        self._tokens = self._capacity
        self._last_check = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, tokens: int, stop_event: threading.Event) -> bool:
        while not stop_event.is_set():
            with self._lock:
                capacity = max(self._capacity, float(tokens))
                now = time.monotonic()
                elapsed = now - self._last_check
                self._tokens = min(capacity, self._tokens + elapsed * self._rate)
                self._last_check = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                deficit = tokens - self._tokens
                wait_s = min(deficit / self._rate, 0.05)
            stop_event.wait(wait_s)
        return False


class SequenceAllocator:
    def __init__(self, step: int) -> None:
        self._step = step
        self._next = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            value = self._next
            self._next += self._step
            return value


def get_json(url: str, timeout: float = 5) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception:
        return None


def active_flink_job() -> dict[str, Any] | None:
    payload = get_json("http://localhost:8081/jobs/overview")
    if not payload:
        return None
    for job in payload.get("jobs", []):
        if job.get("state") == "RUNNING":
            return job
    jobs = payload.get("jobs", [])
    return jobs[0] if jobs else None


def checkpoint_snapshot() -> dict[str, Any]:
    job = active_flink_job()
    if not job:
        return {}
    jid = str(job.get("jid"))
    payload = get_json(f"http://localhost:8081/jobs/{jid}/checkpoints") or {}
    return {
        "job_id": jid,
        "job_name": job.get("name"),
        "job_state": job.get("state"),
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
        fields = line.rsplit(" ", 1)
        if len(fields) != 2:
            continue
        try:
            max_value = max(max_value, float(fields[1]))
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


def active_slots() -> list[dict[str, Any]]:
    sql = """
        SELECT
            slot_name,
            active,
            COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::BIGINT AS lag_bytes
        FROM pg_replication_slots
        WHERE slot_type = 'logical'
        ORDER BY slot_name
    """
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return [
                {"slot_name": str(name), "active": bool(active), "lag_bytes": int(lag)}
                for name, active, lag in cursor.fetchall()
            ]
    finally:
        conn.close()


class MetricsSampler:
    def __init__(self, interval_s: float = 2.0) -> None:
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.slot_lag_max_bytes = 0
        self.slot_lag_final_bytes = 0
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
            self.slot_lag_max_bytes = max(self.slot_lag_max_bytes, self.slot_lag_final_bytes)
        except Exception:
            pass

        metrics = scrape_prometheus_metrics(9250)
        if not metrics:
            return
        self.backpressured_ms_per_s_max = max(
            self.backpressured_ms_per_s_max,
            max_metric_value(metrics, "flink_taskmanager_job_task_backPressuredTimeMsPerSecond"),
        )
        self.soft_backpressured_ms_per_s_max = max(
            self.soft_backpressured_ms_per_s_max,
            max_metric_value(metrics, "flink_taskmanager_job_task_softBackPressuredTimeMsPerSecond"),
        )
        self.busy_ms_per_s_max = max(
            self.busy_ms_per_s_max,
            max_metric_value(metrics, "flink_taskmanager_job_task_busyTimeMsPerSecond"),
        )
        self.is_backpressured_max = max(
            self.is_backpressured_max,
            max_metric_value(metrics, "flink_taskmanager_job_task_isBackPressured"),
        )


def execute_batch(cur, run_id: str, start_seq: int, ops: list[str]) -> None:
    for offset, op in enumerate(ops):
        stress_stream.do_one_event_without_commit(cur, run_id, start_seq + offset, op)


def choose_ops(batch_size: int) -> list[str]:
    return random.choices(
        stress_stream.OPS,
        weights=stress_stream.OP_WEIGHTS,
        k=batch_size,
    )


def writer_worker(
    worker_id: int,
    run_id: str,
    batch_size: int,
    limiter: GlobalBatchRateLimiter,
    start_monotonic: float,
    warmup_end: float,
    stop_event: threading.Event,
    sequences: SequenceAllocator,
    stats: WorkerStats,
) -> None:
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        while not stop_event.is_set():
            if not limiter.consume(batch_size, stop_event):
                break
            start_seq = sequences.next()
            ops = choose_ops(batch_size)
            commit_started = time.monotonic()
            try:
                execute_batch(cur, run_id, start_seq, ops)
                conn.commit()
                committed = time.monotonic()
                stats.total_events += batch_size
                stats.batches += 1
                if committed >= warmup_end:
                    stats.measured_events += batch_size
                    stats.measured_batches += 1
                    stats.commit_latencies_s.append(committed - commit_started)
            except Exception as exc:
                stats.errors += 1
                stats.retries += 1
                conn.rollback()
                elapsed = time.monotonic() - start_monotonic
                print(f"[worker {worker_id}] t={elapsed:.1f}s error: {exc}", flush=True)
                stop_event.wait(0.2)
    finally:
        cur.close()
        conn.close()


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
    return ProbeResult(probe_id, committed, committed - started)


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
            WHERE probe_id IN ({",".join(str(probe_id) for probe_id in probe_ids)})
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


def probe_worker(
    run_id: str,
    interval_s: float,
    timeout_s: float,
    poll_interval_s: float,
    stop_event: threading.Event,
    results: list[ProbeResult],
) -> None:
    if interval_s <= 0:
        return
    clickhouse = ClickHouseClient()
    conn = get_conn()
    conn.autocommit = False
    pending: dict[int, ProbeResult] = {}
    deadlines: dict[int, float] = {}
    probe_key = f"true-throughput-{run_id}-{socket.gethostname()}"
    next_insert_at = time.monotonic()
    try:
        while not stop_event.is_set() or pending:
            now = time.monotonic()
            if not stop_event.is_set() and now >= next_insert_at:
                index = len(results) + 1
                payload = f"{probe_key}:{index}:{time.time_ns()}"
                try:
                    result = insert_probe(conn, probe_key, payload)
                    results.append(result)
                    pending[result.probe_id] = result
                    deadlines[result.probe_id] = result.committed_monotonic + timeout_s
                except Exception as exc:
                    print(f"[probe] error: {exc}", flush=True)
                    conn.rollback()
                next_insert_at += interval_s

            try:
                poll_probe_results(clickhouse, pending, now)
            except Exception:
                pass

            expired = [
                probe_id
                for probe_id, deadline in deadlines.items()
                if probe_id in pending and now >= deadline
            ]
            for probe_id in expired:
                pending[probe_id].timed_out = True
                pending.pop(probe_id, None)

            if pending or not stop_event.is_set():
                stop_event.wait(poll_interval_s)
    finally:
        conn.close()


def clickhouse_row_counts() -> dict[str, int]:
    client = ClickHouseClient()
    rows = client.query(
        """
        SELECT name, sum(total_rows)
        FROM system.tables
        WHERE database = 'ecommerce_ods'
          AND name IN (
            'orders_sink',
            'payments_sink',
            'shipments_sink',
            'products_sink',
            'inventory_movements_sink',
            'cdc_latency_probe_sink'
          )
        GROUP BY name
        """
    )
    counts: dict[str, int] = {}
    if not rows:
        return counts
    for row in rows.splitlines():
        name, count = row.split("\t")
        counts[name] = int(count)
    return counts


def clickhouse_parts() -> dict[str, int]:
    client = ClickHouseClient()
    rows = client.query(
        """
        SELECT table, count()
        FROM system.parts
        WHERE database = 'ecommerce_ods'
          AND active
        GROUP BY table
        """
    )
    parts: dict[str, int] = {}
    if not rows:
        return parts
    for row in rows.splitlines():
        table, count = row.split("\t")
        parts[table] = int(count)
    return parts


def active_merges() -> int | None:
    client = ClickHouseClient()
    try:
        value = client.query("SELECT count() FROM system.merges WHERE database = 'ecommerce_ods'")
        return int(value or "0")
    except Exception:
        return None


def wait_for_catchup(timeout_s: float, threshold_bytes: int = 1_000_000) -> float | None:
    deadline = time.monotonic() + timeout_s
    started = time.monotonic()
    while time.monotonic() <= deadline:
        lags = all_slot_lags()
        if max(lags.values(), default=0) < threshold_bytes:
            return time.monotonic() - started
        time.sleep(2)
    return None


def run_validation(skip: bool) -> str:
    if skip:
        return "SKIPPED"
    timeout_s = float(os.getenv("TRUE_THROUGHPUT_VALIDATION_TIMEOUT", "60"))
    interval_s = float(os.getenv("TRUE_THROUGHPUT_VALIDATION_INTERVAL", "3"))
    deadline = time.monotonic() + timeout_s
    results = []
    while True:
        try:
            results = compare_postgres_clickhouse.run_check_results_once()
        except Exception as exc:
            return f"ERROR: {exc}"
        if all(result.passed for result in results):
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(interval_s)
    passed = sum(1 for result in results if result.passed)
    failed = len(results) - passed
    if failed == 0:
        return f"{passed}/{len(results)} PASS"
    failed_names = ", ".join(result.check.name for result in results if not result.passed)
    return f"{passed}/{len(results)} PASS; failed: {failed_names}"


def percentile(samples: MetricSamples, percent: int) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, math.ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


def metric_summary(samples: MetricSamples) -> dict[str, float]:
    return {
        "p50": percentile(samples, 50),
        "p95": percentile(samples, 95),
        "p99": percentile(samples, 99),
        "max": max(samples) if samples else 0.0,
    }


def checkpoint_delta(before: dict[str, Any], after: dict[str, Any], key: str) -> int:
    before_counts = before.get("counts", {}) if before else {}
    after_counts = after.get("counts", {}) if after else {}
    try:
        return int(after_counts.get(key, 0)) - int(before_counts.get(key, 0))
    except Exception:
        return 0


def checkpoint_p95_ms(snapshot: dict[str, Any]) -> int | None:
    try:
        return int(snapshot.get("summary", {}).get("end_to_end_duration", {}).get("p95"))
    except Exception:
        return None


def fmt_seconds(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}s"


def fmt_ms(value: float) -> str:
    return f"{value * 1000:.1f} ms"


def fmt_bytes(value: int) -> str:
    current = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if current < 1024 or unit == "GB":
            return f"{current:.1f} {unit}" if unit != "B" else f"{int(current)} B"
        current /= 1024
    return f"{value} B"


def classify_result(result: RunResult) -> str:
    target_low = result.rate_per_min * 0.95
    target_high = result.rate_per_min * 1.05
    rate_ok = target_low <= result.actual_rate_per_min <= target_high
    job_running = result.checkpoint_after.get("job_state") == "RUNNING"
    failed_delta = checkpoint_delta(result.checkpoint_before, result.checkpoint_after, "failed")
    latency_p95 = percentile(result.source_to_sink_latencies_s, 95)
    validation_skipped = result.validation_summary == "SKIPPED"
    validation_ok = (
        validation_skipped
        or ("PASS" in result.validation_summary and "failed:" not in result.validation_summary)
    )
    catchup_ok = result.catchup_duration_s is not None
    backpressure_high = result.backpressured_ms_per_s_max > 500

    if (
        not job_running
        or result.validation_summary.startswith("ERROR")
        or (not validation_ok and not validation_skipped)
    ):
        return "BREAKING"
    if validation_skipped:
        return "INCOMPLETE"
    if not rate_ok:
        return "DRIVER BOTTLENECK"
    if (
        latency_p95 > 10
        or not catchup_ok
        or failed_delta > 0
        or backpressure_high
        or not validation_ok
    ):
        return "SATURATION"
    return "PASS"


def row_growth(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    keys = sorted(set(before) | set(after))
    return {key: after.get(key, 0) - before.get(key, 0) for key in keys}


def run_true_throughput(
    *,
    rate_per_min: int,
    duration_s: int,
    warmup_s: int,
    workers: int,
    batch_size: int,
    probe_interval_s: float,
    probe_timeout_s: float,
    settle_timeout_s: float,
    skip_validation: bool,
    run_id: str,
) -> RunResult:
    started_at = datetime.now(timezone.utc).isoformat()
    checkpoint_before = checkpoint_snapshot()
    ch_counts_before = clickhouse_row_counts()
    sampler = MetricsSampler()
    limiter = GlobalBatchRateLimiter(rate_per_min / 60.0)
    stop_event = threading.Event()
    sequences = SequenceAllocator(batch_size)
    probe_results: list[ProbeResult] = []
    worker_stats = [WorkerStats(worker_id=index + 1) for index in range(workers)]

    start = time.monotonic()
    warmup_end = start + warmup_s
    probe = threading.Thread(
        target=probe_worker,
        args=(run_id, probe_interval_s, probe_timeout_s, 0.2, stop_event, probe_results),
        daemon=True,
    )
    writer_threads = [
        threading.Thread(
            target=writer_worker,
            args=(
                stat.worker_id,
                run_id,
                batch_size,
                limiter,
                start,
                warmup_end,
                stop_event,
                sequences,
                stat,
            ),
            daemon=True,
        )
        for stat in worker_stats
    ]

    print(
        "True throughput started: "
        f"rate={rate_per_min}/min duration={duration_s}s warmup={warmup_s}s "
        f"workers={workers} batch_size={batch_size} run_id={run_id}",
        flush=True,
    )
    sampler.start()
    probe.start()
    for thread in writer_threads:
        thread.start()

    next_progress = start + 10
    try:
        while time.monotonic() - start < duration_s:
            now = time.monotonic()
            if now >= next_progress:
                measured = sum(stat.measured_events for stat in worker_stats)
                total = sum(stat.total_events for stat in worker_stats)
                errors = sum(stat.errors for stat in worker_stats)
                elapsed_measured = max(1.0, now - warmup_end)
                rate = (measured / elapsed_measured) * 60 if now >= warmup_end else 0.0
                print(
                    f"  t={now - start:.0f}s total={total} measured={measured} "
                    f"actual={rate:.1f}/min errors={errors} "
                    f"lag={fmt_bytes(sampler.slot_lag_final_bytes)}",
                    flush=True,
                )
                next_progress = now + 10
            time.sleep(0.2)
    finally:
        stop_event.set()
        for thread in writer_threads:
            thread.join(timeout=15)
        probe.join(timeout=max(5.0, probe_timeout_s + 5.0))
        sampler.stop()

    checkpoint_after = checkpoint_snapshot()
    catchup_duration_s = wait_for_catchup(settle_timeout_s)
    ch_counts_after = clickhouse_row_counts()
    validation_summary = run_validation(skip_validation)

    measured_duration = max(1.0, duration_s - warmup_s)
    measured_events = sum(stat.measured_events for stat in worker_stats)
    commit_latencies = [
        value for stat in worker_stats for value in stat.commit_latencies_s
    ]
    source_latencies = [
        result.source_to_sink_s for result in probe_results if result.source_to_sink_s is not None
    ]
    client_latencies = [
        result.client_observed_s for result in probe_results if result.client_observed_s is not None
    ]
    probe_commit_latencies = [result.commit_latency_s for result in probe_results]

    result = RunResult(
        run_id=run_id,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat(),
        rate_per_min=rate_per_min,
        duration_s=duration_s,
        warmup_s=warmup_s,
        workers=workers,
        batch_size=batch_size,
        total_events=sum(stat.total_events for stat in worker_stats),
        measured_events=measured_events,
        actual_rate_per_min=(measured_events / measured_duration) * 60,
        writer_errors=sum(stat.errors for stat in worker_stats),
        retry_count=sum(stat.retries for stat in worker_stats),
        commit_latencies_s=commit_latencies,
        probe_requested=len(probe_results),
        probe_observed=len(source_latencies),
        probe_timeouts=sum(1 for result in probe_results if result.timed_out),
        probe_commit_latencies_s=probe_commit_latencies,
        source_to_sink_latencies_s=source_latencies,
        client_observed_latencies_s=client_latencies,
        max_slot_lag_bytes=sampler.slot_lag_max_bytes,
        final_slot_lag_bytes=sampler.slot_lag_final_bytes,
        catchup_duration_s=catchup_duration_s,
        active_slots=active_slots(),
        checkpoint_before=checkpoint_before,
        checkpoint_after=checkpoint_after,
        backpressured_ms_per_s_max=sampler.backpressured_ms_per_s_max,
        soft_backpressured_ms_per_s_max=sampler.soft_backpressured_ms_per_s_max,
        busy_ms_per_s_max=sampler.busy_ms_per_s_max,
        is_backpressured_max=sampler.is_backpressured_max,
        sink_row_growth=row_growth(ch_counts_before, ch_counts_after),
        clickhouse_parts=clickhouse_parts(),
        active_merges=active_merges(),
        validation_summary=validation_summary,
        conclusion="",
    )
    result.conclusion = classify_result(result)
    return result


def write_report(path: Path, result: RunResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    commit = metric_summary(result.commit_latencies_s)
    source = metric_summary(result.source_to_sink_latencies_s)
    client = metric_summary(result.client_observed_latencies_s)
    probe_commit = metric_summary(result.probe_commit_latencies_s)
    completed_delta = checkpoint_delta(result.checkpoint_before, result.checkpoint_after, "completed")
    failed_delta = checkpoint_delta(result.checkpoint_before, result.checkpoint_after, "failed")
    checkpoint_p95 = checkpoint_p95_ms(result.checkpoint_after)

    slot_lines = [
        f"- {slot['slot_name']}: active={slot['active']} lag={fmt_bytes(slot['lag_bytes'])}"
        for slot in result.active_slots
    ]
    growth_lines = [
        f"- {table}: {growth}" for table, growth in sorted(result.sink_row_growth.items())
    ]
    part_lines = [
        f"- {table}: {parts}" for table, parts in sorted(result.clickhouse_parts.items())
    ]

    lines = [
        f"# True Throughput Result - {result.rate_per_min} events/min",
        "",
        f"Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Run ID: `{result.run_id}`",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Target rate | {result.rate_per_min}/min |",
        f"| Actual rate | {result.actual_rate_per_min:.1f}/min |",
        f"| Duration / warmup | {result.duration_s}s / {result.warmup_s}s |",
        f"| Workers | {result.workers} |",
        f"| Batch size | {result.batch_size} |",
        f"| Total events | {result.total_events} |",
        f"| Measured events | {result.measured_events} |",
        f"| Writer errors | {result.writer_errors} |",
        f"| Retry count | {result.retry_count} |",
        f"| Commit latency p50 | {fmt_ms(commit['p50'])} |",
        f"| Commit latency p95 | {fmt_ms(commit['p95'])} |",
        f"| Commit latency p99 | {fmt_ms(commit['p99'])} |",
        f"| Probe samples observed | {result.probe_observed}/{result.probe_requested} |",
        f"| Probe timeouts | {result.probe_timeouts} |",
        f"| Probe commit p95 | {fmt_ms(probe_commit['p95'])} |",
        f"| Latency p50 | {fmt_seconds(source['p50'])} |",
        f"| Latency p95 | {fmt_seconds(source['p95'])} |",
        f"| Latency p99 | {fmt_seconds(source['p99'])} |",
        f"| Latency max | {fmt_seconds(source['max'])} |",
        f"| Client-observed p95 | {fmt_seconds(client['p95'])} |",
        f"| Max slot lag | {fmt_bytes(result.max_slot_lag_bytes)} |",
        f"| Final slot lag | {fmt_bytes(result.final_slot_lag_bytes)} |",
        f"| Catch-up duration | {fmt_seconds(result.catchup_duration_s)} |",
        f"| Checkpoint completed delta | {completed_delta} |",
        f"| Checkpoint failed delta | {failed_delta} |",
        f"| Checkpoint duration p95 | {checkpoint_p95 if checkpoint_p95 is not None else 'n/a'} ms |",
        f"| Backpressure max | {result.backpressured_ms_per_s_max:.1f} ms/s |",
        f"| Busy time max | {result.busy_ms_per_s_max:.1f} ms/s |",
        f"| Active merges | {result.active_merges if result.active_merges is not None else 'n/a'} |",
        f"| Validation result | {result.validation_summary} |",
        f"| Conclusion | {result.conclusion} |",
        "",
        "## Active Logical Slots",
        "",
        *(slot_lines or ["- n/a"]),
        "",
        "## ClickHouse Sink Row Growth",
        "",
        *(growth_lines or ["- n/a"]),
        "",
        "## ClickHouse Active Parts",
        "",
        *(part_lines or ["- n/a"]),
        "",
        "## Notes",
        "",
        "- Actual rate is measured after warmup only.",
        "- CDC latency is measured in ClickHouse as `sink_observed_at - source_updated_at`.",
        "- Validation is run after settle/catch-up unless `--skip-validation` is used.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="True CDC throughput test with multi-threaded batch writers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--rate", type=positive_int, default=1000)
    parser.add_argument("--duration", type=positive_int, default=600)
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--workers", type=positive_int, default=4)
    parser.add_argument("--batch-size", type=positive_int, default=10)
    parser.add_argument("--probe-interval", type=float, default=1.0)
    parser.add_argument("--probe-timeout", type=float, default=30.0)
    parser.add_argument("--settle-timeout", type=float, default=300.0)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--skip-validation", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.warmup < 0:
        print("ERROR: --warmup must be greater than or equal to 0", file=sys.stderr)
        return 2
    if args.duration <= args.warmup:
        print("ERROR: --duration must be greater than --warmup", file=sys.stderr)
        return 2
    if args.probe_interval < 0:
        print("ERROR: --probe-interval must be greater than or equal to 0", file=sys.stderr)
        return 2

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output_path = args.output or Path(
        f"docs/Version 4/RESULT_TRUE_{args.rate}epm_{run_id}.md"
    )

    result = run_true_throughput(
        rate_per_min=args.rate,
        duration_s=args.duration,
        warmup_s=args.warmup,
        workers=args.workers,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval,
        probe_timeout_s=args.probe_timeout,
        settle_timeout_s=args.settle_timeout,
        skip_validation=args.skip_validation,
        run_id=run_id,
    )
    write_report(output_path, result)
    print(
        "True throughput done: "
        f"actual={result.actual_rate_per_min:.1f}/min "
        f"events={result.measured_events} errors={result.writer_errors} "
        f"latency_p95={fmt_seconds(percentile(result.source_to_sink_latencies_s, 95))} "
        f"conclusion={result.conclusion}",
        flush=True,
    )
    print(f"Report written to: {output_path}", flush=True)
    return 0 if result.conclusion in {"PASS", "SATURATION", "INCOMPLETE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
