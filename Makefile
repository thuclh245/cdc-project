PYTHON ?= $(if $(wildcard venv/bin/python),venv/bin/python,python3)
COMPOSE ?= docker compose

.PHONY: help up down reset ps logs-flink seed stream pg ch validate verify ready wait-job latency

help:
	@echo "Available commands:"
	@echo "  make up          Build and start all services"
	@echo "  make down        Stop all services"
	@echo "  make reset       Recreate services and delete persisted data"
	@echo "  make ps          Show service status"
	@echo "  make logs-flink  Show the latest Flink logs"
	@echo "  make seed        Seed PostgreSQL"
	@echo "  make stream      Start fake data streams"
	@echo "  make pg          Open a PostgreSQL shell"
	@echo "  make ch          Open a ClickHouse shell"
	@echo "  make validate    Compare PostgreSQL and ClickHouse"
	@echo "  make latency     Measure PostgreSQL to ClickHouse CDC latency"
up:
	$(COMPOSE) up -d --build
	$(MAKE) wait-job
	$(MAKE) ready

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d --build
	$(MAKE) wait-job
	$(MAKE) ready

wait-job:
	$(COMPOSE) wait flink-job-submitter

ps:
	$(COMPOSE) ps

logs-flink:
	$(COMPOSE) logs --tail=200 flink-jobmanager flink-taskmanager

seed:
	$(PYTHON) -m scripts.run_seed

stream:
	$(PYTHON) -m scripts.main_stream

pg:
	docker exec -it pg-primary psql -U postgres -d ecommerce_ods

ch:
	docker exec -it clickhouse-sink clickhouse-client

validate:
	$(PYTHON) -m scripts.validation.compare_postgres_clickhouse

latency:
	$(PYTHON) -m scripts.benchmark.cdc_latency_benchmark --samples 100 --interval 1

verify:
	$(PYTHON) -m scripts.validation.verify_stack

ready:
	$(PYTHON) -m scripts.validation.verify_stack --readiness
