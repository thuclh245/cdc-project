PYTHON ?= $(if $(wildcard venv/bin/python),venv/bin/python,python3)
COMPOSE ?= docker compose
VERSION3_REPORT_DIR ?= docs/Version 3

.PHONY: help up down reset ps logs-flink minio-state seed stream pg pg-roles ch validate validate-no-history validation-history validation-check-history verify ready wait-job latency large-load benchmark-300mb benchmark-500mb benchmark-2gb benchmark-3gb benchmark-4gb working-data-300mb fault-tolerance flink-recovery pg-failover-test

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
	@echo "  make flink-recovery Run Version 3 Phase 2 Flink recovery test"
	@echo "  make pg-failover-test Run Version 3 Phase 7 PostgreSQL HA failover test"
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
	docker exec pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods -c "SELECT run_id, started_at, duration_seconds, status, passed_checks, failed_checks, attempts, source, error_message FROM cdc_validation_runs ORDER BY started_at DESC LIMIT 10;"

validation-check-history:
	docker exec pg-node-1 psql -h pg-haproxy -p 5432 -U postgres -d ecommerce_ods -c "SELECT r.run_id, c.check_name, c.passed, c.postgres_value, c.clickhouse_value, c.mismatch_detail FROM cdc_validation_check_results c JOIN cdc_validation_runs r ON r.run_id = c.run_id ORDER BY r.started_at DESC, c.check_name LIMIT 30;"

latency:
	$(PYTHON) -m scripts.benchmark.cdc_latency_benchmark --samples 100 --interval 1

large-load:
	$(PYTHON) -m scripts.benchmark.large_data_load $(ARGS)

benchmark-300mb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 300MB --latency-samples 10 --wait-timeout 180 --report-file "$(VERSION3_REPORT_DIR)/08_PHASE_6_CAPACITY_PLANNING_RESULT_300MB.md"

benchmark-500mb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 500MB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_PHASE_6_CAPACITY_PLANNING_RESULT_500MB.md"

benchmark-2gb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 2GB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_PHASE_6_CAPACITY_PLANNING_RESULT_2GB.md"

benchmark-3gb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 3GB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_PHASE_6_CAPACITY_PLANNING_RESULT_3GB.md"

benchmark-4gb:
	$(PYTHON) -m scripts.benchmark.large_data_load --target-size 4GB --latency-samples 20 --wait-timeout 300 --report-file "$(VERSION3_REPORT_DIR)/08_PHASE_6_CAPACITY_PLANNING_RESULT_4GB.md"

working-data-300mb:
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) up -d --build --remove-orphans
	$(MAKE) wait-job
	$(MAKE) ready
	$(MAKE) benchmark-300mb

fault-tolerance:
	$(PYTHON) -m scripts.validation.fault_tolerance_test --scenario all

flink-recovery:
	$(PYTHON) -m scripts.validation.flink_recovery_test --include-jobmanager --resubmit-after-jobmanager --report-file "$(VERSION3_REPORT_DIR)/04_PHASE_2_FLINK_RECOVERY_RESULT_2026-07-03.md"

pg-failover-test:
	$(PYTHON) -m scripts.validation.postgres_failover_test --report-file "$(VERSION3_REPORT_DIR)/10_PHASE_7_POSTGRES_HA_RESULT_2026-07-03.md"

verify:
	$(PYTHON) -m scripts.validation.verify_stack

ready:
	$(PYTHON) -m scripts.validation.verify_stack --readiness
