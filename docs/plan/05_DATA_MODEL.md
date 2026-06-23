# 05. Thiết kế dữ liệu


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## PostgreSQL source table

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status VARCHAR(30) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted BOOLEAN NOT NULL DEFAULT FALSE
);

ALTER TABLE orders REPLICA IDENTITY FULL;
```

## Ý nghĩa cột

| Cột | Vai trò |
|---|---|
| `order_id` | Khóa nghiệp vụ, dùng đảm bảo idempotent |
| `customer_id` | Mã khách hàng |
| `status` | Trạng thái đơn hàng |
| `amount` | Giá trị đơn hàng |
| `created_at` | Thời điểm tạo đơn |
| `updated_at` | Event-time/version |
| `deleted` | Soft delete |

## Trạng thái đơn hàng

```text
CREATED → PAID → SHIPPED → COMPLETED
CREATED → CANCELLED
```

## Soft delete

Không ưu tiên xóa vật lý. Khi hủy đơn:

```sql
UPDATE orders
SET status = 'CANCELLED',
    deleted = TRUE,
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 1001;
```

Lý do:

- Phù hợp OLAP.
- Giữ lịch sử nghiệp vụ.
- Dễ thống kê đơn hủy.
- Dễ đảm bảo idempotent.

## ClickHouse sink table

```sql
CREATE DATABASE IF NOT EXISTS ecommerce_ods;

CREATE TABLE IF NOT EXISTS ecommerce_ods.orders_sink
(
    order_id UInt64,
    customer_id UInt64,
    status String,
    amount Decimal(12, 2),
    created_at DateTime,
    updated_at DateTime,
    deleted UInt8
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;
```

## Chiến lược idempotent

| Thành phần | Thiết kế |
|---|---|
| Khóa | `order_id` |
| Version | `updated_at` |
| Delete | `deleted` |
| Engine | `ReplacingMergeTree(updated_at)` |
| Query trạng thái cuối | `FINAL` |

Query kiểm tra trạng thái mới nhất:

```sql
SELECT *
FROM ecommerce_ods.orders_sink FINAL
WHERE order_id = 1001;
```
