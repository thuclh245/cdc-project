# 06. Triển khai Docker Compose


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Service và port

| Service | Container | Port host | Vai trò |
|---|---|---:|---|
| `pg-primary` | `pg-primary` | 5433 | Primary HA |
| `pg-replica-1` | `pg-replica-1` | 5434 | Replica 1 |
| `pg-replica-2` | `pg-replica-2` | 5435 | Replica 2 |
| `pg-haproxy` | `pg-haproxy` | 15432/15433 | Proxy |
| `flink-jobmanager` | `flink-jobmanager` | 8081 | Flink UI/JobManager |
| `clickhouse` | `clickhouse-sink` | 8123/9000 | ClickHouse |

## PostgreSQL cấu hình CDC

Các cấu hình quan trọng:

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
listen_addresses=*
```

Ý nghĩa:

- `wal_level=logical`: cần cho Flink CDC.
- `max_wal_senders`: phục vụ replica và CDC consumer.
- `max_replication_slots`: cho phép slot logical/physical.
- `listen_addresses=*`: cho phép container khác kết nối.

## Flink Dockerfile riêng

```dockerfile
FROM flink:1.18.1-scala_2.12-java11
COPY connectors/*.jar /opt/flink/lib/
```

Lý do cần Dockerfile riêng: Flink image gốc chỉ là engine, chưa có connector PostgreSQL CDC và ClickHouse/JDBC. File `.jar` là driver/connector giúp Flink hiểu cách đọc PostgreSQL và ghi ClickHouse.

## Volumes

| Volume | Vai trò |
|---|---|
| `pg_primary_data` | Dữ liệu primary |
| `pg_replica1_data` | Dữ liệu replica 1 |
| `pg_replica2_data` | Dữ liệu replica 2 |
| `clickhouse_data` | Dữ liệu ClickHouse |
| `flink_checkpoints` | Checkpoint Flink |

## Network

```yaml
networks:
  cdc-network:
    driver: bridge
```

Các container cùng network có thể gọi nhau bằng tên: `pg-primary`, `flink-jobmanager`, `clickhouse-sink`.

## Lệnh triển khai

```powershell
docker compose up -d --build
docker ps
docker logs pg-primary
docker logs flink-jobmanager
```

Reset sạch khi cần:

```powershell
docker compose down -v
docker compose up -d --build
```
