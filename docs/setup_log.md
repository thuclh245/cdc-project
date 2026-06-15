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
