# 27. Operations Runbook

## 1. Mục tiêu

Runbook là tài liệu vận hành: khi cần kiểm tra, restart, debug hoặc xử lý lỗi thì làm gì. Đây là tài liệu giúp dự án có tính production-like thay vì chỉ là demo chạy một lần.

## 2. Start hệ thống

```powershell
docker compose up -d
```

Kiểm tra:

```powershell
docker ps
```

Kỳ vọng có các container:

```text
pg-source
pg-primary
pg-replica-1
pg-replica-2
pg-haproxy
flink-jobmanager
flink-taskmanager
clickhouse-sink
```

## 3. Stop hệ thống

```powershell
docker compose down
```

Xóa cả volume để dựng sạch:

```powershell
docker compose down -v
```

Cảnh báo: `down -v` sẽ xóa dữ liệu PostgreSQL, ClickHouse và checkpoint.

## 4. Kiểm tra PostgreSQL primary

```powershell
docker exec -it pg-primary psql -U postgres -d cdc_demo
```

```sql
SELECT pg_is_in_recovery();
SHOW wal_level;
SELECT * FROM pg_publication;
```

Kỳ vọng:

```text
pg_is_in_recovery = f
wal_level = logical
publication tồn tại
```

## 5. Kiểm tra replica

Replica 1:

```powershell
docker exec -it pg-replica-1 psql -U postgres -d cdc_demo
```

```sql
SELECT pg_is_in_recovery();
SELECT COUNT(*) FROM orders;
```

Replica 2:

```powershell
docker exec -it pg-replica-2 psql -U postgres -d cdc_demo
```

```sql
SELECT pg_is_in_recovery();
SELECT COUNT(*) FROM orders;
```

Kỳ vọng:

```text
pg_is_in_recovery = t
orders đọc được
```

## 6. Kiểm tra replication

Trên primary:

```sql
SELECT application_name, client_addr, state, sync_state
FROM pg_stat_replication;
```

Kỳ vọng: có 2 dòng replica và `state = streaming`.

## 7. Kiểm tra Flink job

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink list
```

Nếu chưa chạy job:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/postgres_to_clickhouse.sql
```

## 8. Kiểm tra ClickHouse

```powershell
docker exec -it clickhouse-sink clickhouse-client
```

```sql
SELECT count() FROM cdc_demo.orders_sink FINAL;

SELECT *
FROM cdc_demo.orders_sink FINAL
ORDER BY updated_at DESC
LIMIT 10;
```

## 9. Test nhanh INSERT/UPDATE/SOFT DELETE

### INSERT

```sql
INSERT INTO orders(order_id, customer_id, status, amount, deleted)
VALUES (990001, 1001, 'CREATED', 100000, FALSE);
```

### UPDATE

```sql
UPDATE orders
SET status = 'PAID', updated_at = CURRENT_TIMESTAMP
WHERE order_id = 990001;
```

### SOFT DELETE

```sql
UPDATE orders
SET deleted = TRUE,
    status = 'CANCELLED',
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 990001;
```

### Check ClickHouse

```sql
SELECT *
FROM cdc_demo.orders_sink FINAL
WHERE order_id = 990001;
```

## 10. Restart TaskManager

```powershell
docker restart flink-taskmanager
```

Kiểm tra job:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink list
```

Insert mới:

```sql
INSERT INTO orders(order_id, customer_id, status, amount, deleted)
VALUES (990002, 1002, 'CREATED', 200000, FALSE);
```

Kiểm tra ClickHouse:

```sql
SELECT * FROM cdc_demo.orders_sink FINAL WHERE order_id = 990002;
```

## 11. Lỗi thường gặp

### Flink không nhận connector

Dấu hiệu:

```text
Could not find any factory for identifier 'postgres-cdc'
```

Cách xử lý:

```powershell
docker exec -it flink-jobmanager ls /opt/flink/lib
docker compose build flink-jobmanager flink-taskmanager
docker compose up -d flink-jobmanager flink-taskmanager
```

### Replica không phải standby

Dấu hiệu: `pg_is_in_recovery()` trên replica trả `f`.

Cách xử lý:

- Xóa volume replica.
- Đảm bảo `pg_basebackup` chạy trước khi PostgreSQL init standalone.
- Kiểm tra user `replicator` và `pg_hba.conf`.

### ClickHouse không nhận dữ liệu

Kiểm tra:

- Flink job có RUNNING không.
- Slot có active không.
- Sink table đúng tên không.
- Log TaskManager có lỗi không.

## 12. Checklist vận hành nhanh

- [ ] `docker ps` tất cả container Up.
- [ ] Primary `pg_is_in_recovery = f`.
- [ ] Replica `pg_is_in_recovery = t`.
- [ ] `pg_stat_replication` có 2 dòng.
- [ ] Flink job RUNNING.
- [ ] Replication slot active.
- [ ] ClickHouse có dữ liệu.
- [ ] Test insert/update/delete pass.
- [ ] Restart TaskManager pass.

## 13. Đoạn viết báo cáo

Dự án xây dựng runbook vận hành gồm các bước kiểm tra trạng thái container, PostgreSQL primary-replica, Flink job, replication slot và dữ liệu trong ClickHouse. Runbook cũng mô tả các thao tác kiểm thử insert, update, soft delete, restart TaskManager và các lỗi thường gặp. Việc có runbook giúp quá trình triển khai có tính lặp lại, dễ debug và gần với quy trình vận hành thực tế.
