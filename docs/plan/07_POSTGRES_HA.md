# 07. PostgreSQL HA


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Mục tiêu

Chứng minh PostgreSQL chạy đúng mô hình:

```text
pg-primary → pg-replica-1
pg-primary → pg-replica-2
```

Primary nhận ghi, replica là standby đọc dữ liệu từ primary thông qua physical WAL streaming.

## Primary init cần có

```sql
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'repl_password';

CREATE TABLE IF NOT EXISTS orders (...);
ALTER TABLE orders REPLICA IDENTITY FULL;

DROP PUBLICATION IF EXISTS orders_pub;
CREATE PUBLICATION orders_pub FOR TABLE orders;
```

## Replica tạo bằng pg_basebackup

```bash
pg_basebackup -h pg-primary -p 5432 -U replicator   -D /var/lib/postgresql/data -Fp -Xs -P -R
```

Ý nghĩa `-R`: tạo cấu hình standby để replica tự kết nối lại primary.

## Kiểm tra primary

```powershell
docker exec -it pg-primary psql -U postgres -d cdc_demo
```

```sql
SELECT pg_is_in_recovery(); -- mong đợi f
SHOW wal_level;             -- mong đợi logical
SELECT * FROM pg_publication;
```

## Kiểm tra replica

```powershell
docker exec -it pg-replica-1 psql -U postgres -d cdc_demo
```

```sql
SELECT pg_is_in_recovery(); -- mong đợi t
SELECT COUNT(*) FROM orders;
```

Làm tương tự với `pg-replica-2`.

## Kiểm tra replication streaming

Trên primary:

```sql
SELECT application_name, client_addr, state, sync_state
FROM pg_stat_replication;
```

Kỳ vọng: có 2 dòng `state = streaming`.

## Test replication

Primary:

```sql
INSERT INTO orders(order_id, customer_id, status, amount)
VALUES (900001, 100, 'CREATED', 150000);
```

Replica:

```sql
SELECT * FROM orders WHERE order_id = 900001;
```

## Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| Replica `pg_is_in_recovery() = f` | Replica bị init độc lập | Xóa volume replica, chạy lại |
| Replica không có bảng | Không clone từ primary | Reset volume replica |
| `pg_basebackup` lỗi auth | Thiếu user/pg_hba | Kiểm tra `replicator`, `pg_hba.conf` |
| Không có 2 dòng streaming | Replica chưa kết nối | Xem log replica |
