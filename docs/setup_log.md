## PostgreSQL setup

- PostgreSQL version: 16
- Database: cdc_demo
- wal_level: logical
- max_replication_slots: 10
- max_wal_senders: 10
- Table: orders
- Publication: orders_pub
- Test INSERT/UPDATE/DELETE: success

## ClickHouse setup

- ClickHouse container: clickhouse-sink
- Database: cdc_demo
- Table: orders_sink
- Engine: ReplacingMergeTree(updated_at)
- Query test: success

## Flink setup

- Flink version: 1.18
- Mode: standalone Docker
- JobManager UI: http://localhost:8081
- TaskManager connected: yes

## CDC Pipeline

- Connector (source): postgres-cdc (flink-sql-connector-postgres-cdc-3.2.1.jar)
- Connector (sink): clickhouse (flink-sql-connector-clickhouse-1.17.1-8.jar)
- URL format: clickhouse://clickhouse:8123
- Slot name: flink_orders_slot
- Decoding plugin: pgoutput
- Job ID: f2692656efb9c9d51d1266cbddf306a0
- INSERT sync: ✅ working
- UPDATE sync: ✅ working (ReplacingMergeTree deduplication)
- DELETE sync: ❌ ignored (sink.ignore-delete=true by default — Week 2 task)
- Latency: ~1–3 seconds observed
