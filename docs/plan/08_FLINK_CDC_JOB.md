# 08. Flink CDC Job


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Nhiệm vụ

- Đọc CDC từ PostgreSQL primary.
- Bắt `INSERT`, `UPDATE`, soft delete.
- Ghi sang ClickHouse.
- Dùng checkpoint để recovery.
- Dùng savepoint cho dừng/nâng cấp job có kiểm soát.

## Source Flink SQL mẫu

```sql
CREATE TABLE postgres_orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_orders_slot',
    'decoding.plugin.name' = 'pgoutput'
);
```

## Sink SQL mẫu

```sql
CREATE TABLE clickhouse_orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse-sink:8123/ecommerce_ods',
    'table-name' = 'orders_sink',
    'driver' = 'com.clickhouse.jdbc.ClickHouseDriver'
);
```

## Insert job

```sql
INSERT INTO clickhouse_orders
SELECT order_id, customer_id, status, amount, created_at, updated_at, deleted
FROM postgres_orders;
```

## Checkpoint

Checkpoint lưu vị trí đọc WAL và trạng thái Flink để recovery.

Cấu hình gợi ý:

```yaml
state.backend: filesystem
state.checkpoints.dir: file:///opt/flink/checkpoints
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
```

## Savepoint

Tạo savepoint:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink savepoint <job_id> file:///opt/flink/checkpoints/savepoints
```

Stop with savepoint:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink stop --savepointPath file:///opt/flink/checkpoints/savepoints <job_id>
```

## Chạy job

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/postgres_to_clickhouse.sql
```

Kiểm tra:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink list
```

## Kiểm tra slot

```sql
SELECT slot_name, plugin, slot_type, active
FROM pg_replication_slots;
```

Kỳ vọng: `flink_orders_slot`, `pgoutput`, `logical`, `active = t`.
