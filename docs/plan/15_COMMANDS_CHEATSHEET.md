# 15. Commands Cheatsheet


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Docker

```powershell
docker compose up -d --build
docker ps
docker compose down
docker compose down -v
```

## PostgreSQL primary

```powershell
docker exec -it pg-primary psql -U postgres -d ecommerce_ods
```

```sql
SELECT pg_is_in_recovery();
SHOW wal_level;
SELECT * FROM pg_publication;
SELECT application_name, client_addr, state, sync_state FROM pg_stat_replication;
SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;
```

## PostgreSQL replica

```powershell
docker exec -it pg-replica-1 psql -U postgres -d ecommerce_ods
docker exec -it pg-replica-2 psql -U postgres -d ecommerce_ods
```

```sql
SELECT pg_is_in_recovery();
SELECT COUNT(*) FROM orders;
```

## Test data

Các script Python dùng chung cấu hình PostgreSQL dưới đây và mặc định ghi trực tiếp
vào `pg-primary` qua host port `5433`:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5433
export POSTGRES_DB=ecommerce_ods
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

Nếu muốn mọi workload đi qua HAProxy write endpoint, chỉ đổi:

```bash
export POSTGRES_PORT=15432
```

Sau đó chạy generator/workload trong cùng terminal.

```sql
INSERT INTO orders(order_id, customer_id, status, amount)
VALUES (950001, 500, 'CREATED', 150000);

UPDATE orders
SET status = 'PAID', updated_at = CURRENT_TIMESTAMP
WHERE order_id = 950001;

UPDATE orders
SET status = 'CANCELLED', deleted = TRUE, updated_at = CURRENT_TIMESTAMP
WHERE order_id = 950001;
```

## Flink

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/postgres_to_clickhouse.sql
docker exec -it flink-jobmanager /opt/flink/bin/flink list
docker restart flink-taskmanager
```

Savepoint:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink savepoint <job_id> file:///opt/flink/checkpoints/savepoints
```

## ClickHouse

```powershell
docker exec -it clickhouse-sink clickhouse-client
```

```sql
SHOW DATABASES;
SHOW TABLES FROM ecommerce_ods;
SELECT * FROM ecommerce_ods.orders_sink FINAL WHERE order_id = 950001;
SELECT status, count(*) FROM ecommerce_ods.orders_sink FINAL WHERE deleted = 0 GROUP BY status;
```
