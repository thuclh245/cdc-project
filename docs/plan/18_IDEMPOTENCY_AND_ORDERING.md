# 18. Đảm bảo Idempotency và Ordering trong CDC

## 1. Mục tiêu

Trong CDC production-like, cần đảm bảo:

```text
Idempotency: xử lý lặp cùng event nhưng kết quả cuối không sai.
Ordering: các thay đổi của cùng một khóa nghiệp vụ được áp dụng đúng thứ tự logic.
```

## 2. Vì sao CDC dễ bị duplicate?

- Flink TaskManager restart.
- Job recover từ checkpoint.
- Sink ghi thành công nhưng checkpoint chưa hoàn tất.
- Network timeout khiến Flink retry.
- Job chạy lại từ savepoint.

Nếu sink append-only, duplicate có thể làm sai `COUNT` và `SUM`.

## 3. Chiến lược v1 đã chốt

| Thành phần | Lựa chọn |
|---|---|
| Khóa nghiệp vụ | `order_id` |
| Version | `updated_at` |
| Delete | Soft delete bằng `deleted` |
| ClickHouse engine | `ReplacingMergeTree(updated_at)` |
| Query latest | Dùng `FINAL` |

ClickHouse table:

```sql
CREATE TABLE cdc_demo.orders_sink (
    order_id Int64,
    customer_id Int64,
    status String,
    amount Decimal(12,2),
    created_at DateTime,
    updated_at DateTime,
    deleted UInt8
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;
```

Query latest state:

```sql
SELECT *
FROM cdc_demo.orders_sink FINAL
WHERE deleted = 0;
```

## 4. Soft delete

Thay vì xóa vật lý, dùng:

```sql
UPDATE orders
SET deleted = TRUE,
    status = 'CANCELLED',
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 1;
```

Lý do:

- Phù hợp OLAP hơn hard delete.
- Dễ kiểm chứng.
- Giữ được dấu vết nghiệp vụ.
- Kết hợp tốt với `ReplacingMergeTree`.

## 5. Ordering

Luồng đúng:

```text
CREATED → PAID → SHIPPED → COMPLETED
```

Ordering có thể bị ảnh hưởng bởi:

- Parallelism cao.
- Event cùng key đi qua task khác nhau.
- Sink ghi bất đồng bộ.
- Event retry.

Khuyến nghị v1:

```text
Flink parallelism = 1 để dễ kiểm soát và demo.
```

Mở rộng:

```text
Thêm event_version hoặc source_lsn để xác định bản mới nhất chắc chắn hơn.
```

## 6. Version nâng cao

PostgreSQL:

```sql
ALTER TABLE orders ADD COLUMN event_version BIGINT DEFAULT 1;
```

Khi update:

```sql
UPDATE orders
SET status = 'PAID',
    event_version = event_version + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 1;
```

ClickHouse:

```sql
ENGINE = ReplacingMergeTree(event_version)
ORDER BY order_id;
```

Ưu điểm: không phụ thuộc hoàn toàn vào timestamp.

## 7. Test idempotent

### INSERT

```sql
INSERT INTO orders(order_id, customer_id, status, amount, deleted)
VALUES (700001, 1001, 'CREATED', 100000, FALSE);
```

### UPDATE nhiều lần

```sql
UPDATE orders SET status='PAID', updated_at=CURRENT_TIMESTAMP WHERE order_id=700001;
UPDATE orders SET status='SHIPPED', updated_at=CURRENT_TIMESTAMP WHERE order_id=700001;
```

### SOFT DELETE

```sql
UPDATE orders
SET deleted=TRUE, status='CANCELLED', updated_at=CURRENT_TIMESTAMP
WHERE order_id=700001;
```

### Kiểm tra

```sql
SELECT *
FROM cdc_demo.orders_sink FINAL
WHERE order_id = 700001;
```

Kỳ vọng: chỉ trạng thái cuối cùng có ý nghĩa logic.

## 8. Correctness latest state

PostgreSQL:

```sql
SELECT COUNT(*), SUM(amount)
FROM orders
WHERE deleted = FALSE;
```

ClickHouse:

```sql
SELECT COUNT(*), SUM(amount)
FROM cdc_demo.orders_sink FINAL
WHERE deleted = 0;
```

## 9. Checklist

- [ ] `order_id` là primary/business key.
- [ ] Có `updated_at`.
- [ ] Có `deleted`.
- [ ] ClickHouse dùng `ReplacingMergeTree`.
- [ ] Query kiểm chứng dùng `FINAL`.
- [ ] Test update nhiều lần.
- [ ] Test soft delete.
- [ ] Test restart Flink.
- [ ] Ghi rõ giới hạn duplicate vật lý.

## 10. Đoạn viết báo cáo

Hệ thống đảm bảo tính idempotent bằng cách thiết kế bảng sink trong ClickHouse theo khóa nghiệp vụ `order_id` và sử dụng `updated_at` làm version cho `ReplacingMergeTree`. Khi Flink xử lý lại cùng một event do restart hoặc retry, kết quả latest state vẫn được xác định theo bản ghi có version mới nhất. Thao tác delete được xử lý bằng soft delete thông qua cột `deleted`, giúp giữ lịch sử logic và phù hợp hơn với OLAP. Trong phạm vi thực nghiệm, parallelism thấp giúp kiểm soát ordering; trong bản mở rộng có thể bổ sung `event_version` hoặc `source_lsn` để đảm bảo ordering mạnh hơn.
