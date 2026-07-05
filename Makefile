PYTHON ?= $(if $(wildcard venv/bin/python),venv/bin/python,python3)
COMPOSE ?= docker compose
VERSION3_REPORT_DIR ?= docs/Version 3

.PHONY: help up down reset ps logs-flink minio-state seed stream pg pg-roles ch validate validate-no-history validation-history validation-check-history verify ready wait-job latency large-load benchmark-300mb benchmark-500mb benchmark-2gb benchmark-3gb benchmark-4gb working-data-300mb fault-tolerance flink-recovery pg-failover-test idempotency-order-test stress-stream stress-tc1 stress-tc2 stress-tc3 stress-tc4 stress-tc5 true-throughput true-tc-a true-tc-b true-tc-c stress-snapshot

help:
	@echo "Available commands:"
	@echo "  make up          Build and start all services"
	@echo "  make down        Stop all services"
	@echo "  make reset       Recreate services and delete persisted data"
	@echo "  make ps          Show service status"
	@echo "  make logs-flink  Show the latest Flink logs"
	@echo "  make minio-state List Flink checkpoint/savepoint objects in MinIO"
	@echo "  make seed        Seed PostgreSQL"
	@echo "  make stream      Start fake data streams"
	@echo "  make pg          Open a PostgreSQL shell"
	@echo "  make pg-roles    Show Patroni PostgreSQL HA roles"
	@echo "  make ch          Open a ClickHouse shell"
	@echo "  make validate    Compare PostgreSQL and ClickHouse"
	@echo "  make validate-no-history Compare without writing validation history"
	@echo "  make validation-history Show recent validation runs"
	@echo "  make validation-check-history Show recent validation check results"
	@echo "  make latency     Measure PostgreSQL to ClickHouse CDC latency"
	@echo "  make large-load  Run large data load test, pass options with ARGS=\"...\""
	@echo "  make benchmark-300mb Run production-like working dataset benchmark"
	@echo "  make benchmark-500mb Run medium local benchmark"
	@echo "  make benchmark-2gb   Run 2GB benchmark when needed"
	@echo "  make benchmark-3gb   Run 3GB benchmark when needed"
	@echo "  make benchmark-4gb   Run 4GB benchmark when needed"
	@echo "  make working-data-300mb Reset volumes and load a 300MB working dataset"
	@echo "  make fault-tolerance Run CDC fault tolerance restart tests"
	@echo "  make flink-recovery Run Flink checkpoint/savepoint recovery test"
	@echo "  make pg-failover-test Run PostgreSQL HA failover test"
	@echo "  make idempotency-order-test Run latest-state idempotency/order test"
	@echo "  make true-throughput Run multi-threaded batch throughput test"
up:
	$(COMPOSE) up -d --build --remove-orphans
	$(MAKE) wait-job
	$(MAKE) ready

down:
	$(COMPOSE) down --remove-orphans

reset:
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) up -d --build --remove-orphans
	$(MAKE) wait-job
	$(MAKE) ready

wait-job:
	@container_id="$$( $(COMPOSE) ps -a -q flink-job-submitter )"; \
	if [ -z "$$container_id" ]; then \
		echo "flink-job-submitter container not found" >&2; \
		exit 1; \
	fi; \
	docker wait "$$container_id" >/dev/null; \
	exit_code="$$(docker inspect --format '{{.State.ExitCode}}' "$$container_id")"; \
	if [ "$$exit_code" != "0" ]; then \
		$(COMPOSE) logs --tail=120 flink-job-submitter; \
		exit "$$exit_code"; \
	fi

ps:
	$(COMPOSE) ps

logs-flink:
	$(COMPOSE) logs --tail=200 flink-jobmanager flink-taskmanager

minio-state:
	$(COMPOSE) run --rm --entrypoint /bin/sh minio-init -c 'mc alias set local http://minio:9000 "$${MINIO_ROOT_USER}" "$${MINIO_ROOT_PASSWORD}" >/dev/null && mc ls "local/$${MINIO_FLINK_BUCKET}" && mc ls --recursive "local/$${MINIO_FLINK_BUCKET}" | head -50'

seed:
	$(PYTHON) -m scripts.run_seed

stream:
	$(PYTHON) -m scripts.main_stream

pg:
	docker exec -it pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods

pg-roles:
	docker exec pg-node-1 /opt/patroni/bin/patronictl -c /tmp/patroni.yml list

ch:
	docker exec -it clickhouse-sink clickhouse-client

validate:
	$(PYTHON) -m scripts.validation.compare_postgres_clickhouse

validate-no-history:
	VALIDATION_HISTORY_ENABLED=0 $(PYTHON) -m scripts.validation.compare_postgres_clickhouse

validation-history:
	docker exec -e PGPASSWORD=postgres pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods -c "SELECT run_id, started_at, duration_seconds, status, passed_checks, failed_checks, attempts, source, error_message FROM cdc_validation_runs ORDER BY started_at DESC LIMIT 10;"

validation-check-history:
	docker exec -e PGPASSWORD=postgres pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods -c "SELECT r.run_id, c.check_name, c.passed, c.postgres_value, c.clickhouse_value, c.mismatch_detail FROM cdc_validation_check_results c JOIN cdc_validation_runs r ON r.run_id = c.run_id ORDER BY r.started_at DESC, c.check_name LIMIT 30;"

latency:
	$(PYTHON) -m scripts.benchmark.cdc_latency_benchmark --samples 100 --interval 1

large-load:
	$(PYTHON) -m scripts.benchmark.large_data_load $(ARGS)

benchmark-300mb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 300MB --latency-samples 10 --wait-timeout 180 --report-file "$(VERSION3_REPORT_DIR)/08_CAPACITY_PLANNING_RESULT_300MB.md"

benchmark-500mb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 500MB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_CAPACITY_PLANNING_RESULT_500MB.md"

benchmark-2gb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 2GB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_CAPACITY_PLANNING_RESULT_2GB.md"

benchmark-3gb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 3GB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_CAPACITY_PLANNING_RESULT_3GB.md"

benchmark-4gb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 4GB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_CAPACITY_PLANNING_RESULT_4GB.md"

working-data-300mb:
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) up -d --build --remove-orphans
	$(MAKE) wait-job
	$(MAKE) ready
	$(MAKE) benchmark-300mb

fault-tolerance:
	$(PYTHON) -m scripts.validation.fault_tolerance_test --scenario all

flink-recovery:
	$(PYTHON) -m scripts.validation.flink_recovery_test --include-jobmanager --resubmit-after-jobmanager --report-file "$(VERSION3_REPORT_DIR)/04_FLINK_RECOVERY_RESULT_2026-07-03.md"

pg-failover-test:
	$(PYTHON) -m scripts.validation.postgres_failover_test --report-file "$(VERSION3_REPORT_DIR)/10_POSTGRES_HA_FAILOVER_RESULT_2026-07-03.md"

idempotency-order-test:
	$(PYTHON) -m scripts.validation.idempotency_order_test --report-file "$(VERSION3_REPORT_DIR)/11_IDEMPOTENCY_ORDER_RESULT_2026-07-03.md"

verify:
	$(PYTHON) -m scripts.validation.verify_stack

ready:
	$(PYTHON) -m scripts.validation.verify_stack --readiness

# ── Version 4: Streaming throughput stress tests ──────────────────────────────
VERSION4_REPORT_DIR ?= docs/Version 4
STRESS_RATE ?= 1000
STRESS_DURATION ?= 600
STRESS_WARMUP ?= 60
TRUE_RATE ?= 1000
TRUE_DURATION ?= 600
TRUE_WARMUP ?= 60
TRUE_WORKERS ?= 4
TRUE_BATCH_SIZE ?= 10
TRUE_PROBE_INTERVAL ?= 1
TRUE_SETTLE_TIMEOUT ?= 300

stress-stream:
	$(PYTHON) -m scripts.benchmark.stress_stream \
		--rate $(STRESS_RATE) \
		--duration $(STRESS_DURATION) \
		--warmup $(STRESS_WARMUP) \
		--output "$(VERSION4_REPORT_DIR)/RESULT_stress_$(STRESS_RATE)epm.md"

# Individual test case targets (cumulative — do NOT reset between runs)
stress-tc1:
	$(PYTHON) -m scripts.benchmark.stress_stream \
		--rate 100 --duration 600 --warmup 60 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TC1_100epm.md"

stress-tc2:
	$(PYTHON) -m scripts.benchmark.stress_stream \
		--rate 500 --duration 600 --warmup 60 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TC2_500epm.md"

stress-tc3:
	$(PYTHON) -m scripts.benchmark.stress_stream \
		--rate 1000 --duration 600 --warmup 60 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TC3_1000epm.md"

stress-tc4:
	$(PYTHON) -m scripts.benchmark.stress_stream \
		--rate 2000 --duration 600 --warmup 60 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TC4_2000epm.md"

stress-tc5:
	$(PYTHON) -m scripts.benchmark.stress_stream \
		--rate 5000 --duration 600 --warmup 60 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TC5_5000epm.md"

true-throughput:
	$(PYTHON) -m scripts.benchmark.true_throughput_stream \
		--rate $(TRUE_RATE) \
		--duration $(TRUE_DURATION) \
		--warmup $(TRUE_WARMUP) \
		--workers $(TRUE_WORKERS) \
		--batch-size $(TRUE_BATCH_SIZE) \
		--probe-interval $(TRUE_PROBE_INTERVAL) \
		--settle-timeout $(TRUE_SETTLE_TIMEOUT) \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TRUE_$(TRUE_RATE)epm.md"

true-tc-a:
	$(PYTHON) -m scripts.benchmark.true_throughput_stream \
		--rate 1000 --duration 600 --warmup 60 \
		--workers 4 --batch-size 10 --probe-interval 1 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TRUE_TC_A_1000epm.md"

true-tc-b:
	$(PYTHON) -m scripts.benchmark.true_throughput_stream \
		--rate 2000 --duration 600 --warmup 60 \
		--workers 6 --batch-size 20 --probe-interval 1 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TRUE_TC_B_2000epm.md"

true-tc-c:
	$(PYTHON) -m scripts.benchmark.true_throughput_stream \
		--rate 5000 --duration 600 --warmup 60 \
		--workers 10 --batch-size 30 --probe-interval 1 \
		--output "$(VERSION4_REPORT_DIR)/RESULT_TRUE_TC_C_5000epm.md"

# Snapshot slot lag and disk usage before/after stress tests
stress-snapshot:
	@echo "=== PostgreSQL slot lag ==="
	docker exec -e PGPASSWORD=postgres pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods \
		-c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag FROM pg_replication_slots WHERE slot_type = 'logical';"
	@echo "=== PostgreSQL DB size ==="
	docker exec -e PGPASSWORD=postgres pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods \
		-c "SELECT pg_size_pretty(pg_database_size('ecommerce_ods')) AS db_size;"
	@echo "=== ClickHouse table sizes ==="
	docker exec clickhouse-sink clickhouse-client \
		--query "SELECT name, formatReadableSize(total_bytes) AS size, sum(total_rows) AS rows FROM system.tables WHERE database = 'ecommerce_ods' GROUP BY name, total_bytes ORDER BY rows DESC;"
	@echo "=== Disk usage ==="
	docker exec pg-node-1 df -h /var/lib/postgresql/data
	docker exec clickhouse-sink df -h /var/lib/clickhouse
