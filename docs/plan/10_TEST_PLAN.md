# 10. Test plan từ A-Z


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Checklist tổng quát

| STT | Test | Kết quả mong đợi |
|---:|---|---|
| 1 | `docker ps` | Tất cả container Up |
| 2 | Primary | `pg_is_in_recovery() = f` |
| 3 | Replica 1 | `pg_is_in_recovery() = t` |
| 4 | Replica 2 | `pg_is_in_recovery() = t` |
| 5 | Replication | `pg_stat_replication` có 2 dòng streaming |
| 6 | Flink | Job RUNNING |
| 7 | Slot | logical slot active |
| 8 | INSERT | ClickHouse nhận dòng mới |
| 9 | UPDATE | ClickHouse cập nhật trạng thái |
| 10 | Soft delete | ClickHouse có `deleted=1` |
| 11 | Correctness | count/sum PostgreSQL = ClickHouse |
| 12 | Latency | < 10 giây |
| 13 | Fault tolerance | Restart TaskManager không mất dữ liệu mới |

## Test INSERT

PostgreSQL:

```sql
INSERT INTO orders(order_id, customer_id, status, amount)
VALUES (920001, 300, 'CREATED', 150000);
```

ClickHouse:

```sql
SELECT * FROM ecommerce_ods.orders_sink FINAL WHERE order_id = 920001;
```

## Test UPDATE

```sql
UPDATE orders
SET status = 'PAID', updated_at = CURRENT_TIMESTAMP
WHERE order_id = 920001;
```

ClickHouse kỳ vọng `status = PAID`.

## Test soft delete

```sql
UPDATE orders
SET status = 'CANCELLED', deleted = TRUE, updated_at = CURRENT_TIMESTAMP
WHERE order_id = 920001;
```

ClickHouse kỳ vọng `deleted = 1`.

## Correctness

PostgreSQL:

```sql
SELECT COUNT(*) FROM orders WHERE deleted = FALSE;
SELECT SUM(amount) FROM orders WHERE deleted = FALSE AND status IN ('PAID','SHIPPED','COMPLETED');
```

ClickHouse:

```sql
SELECT COUNT(*) FROM ecommerce_ods.orders_sink FINAL WHERE deleted = 0;
SELECT SUM(amount) FROM ecommerce_ods.orders_sink FINAL WHERE deleted = 0 AND status IN ('PAID','SHIPPED','COMPLETED');
```

## Latency

Đo từ lúc ghi PostgreSQL đến lúc ClickHouse thấy bản ghi.

Công thức:

```text
latency = thời điểm ClickHouse thấy dữ liệu - thời điểm ghi PostgreSQL
```

## Throughput

Test các mức: 1.000, 10.000, 50.000 dòng.

```text
throughput = số bản ghi / tổng thời gian xử lý
```

## Fault tolerance

```powershell
docker restart flink-taskmanager
```

Sau đó insert bản ghi mới và kiểm tra ClickHouse vẫn nhận được.
