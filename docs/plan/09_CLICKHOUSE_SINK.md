# 09. ClickHouse Sink


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Vai trò

ClickHouse là OLAP sink, nhận dữ liệu từ Flink CDC và phục vụ truy vấn phân tích. Không dùng ClickHouse để xử lý giao dịch người dùng.

## Bảng sink

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

## Vì sao dùng ReplacingMergeTree?

- Cho phép ghi nhiều phiên bản của cùng `order_id`.
- Dùng `updated_at` để chọn bản mới nhất.
- Hỗ trợ chiến lược idempotent.
- Phù hợp với v1 lưu trạng thái mới nhất.

## Query kiểm tra

```sql
SELECT *
FROM ecommerce_ods.orders_sink FINAL
WHERE order_id = 1001;
```

## Query phân tích mẫu

Số đơn theo trạng thái:

```sql
SELECT status, count(*) AS total
FROM ecommerce_ods.orders_sink FINAL
WHERE deleted = 0
GROUP BY status
ORDER BY total DESC;
```

Tổng doanh thu:

```sql
SELECT sum(amount) AS total_revenue
FROM ecommerce_ods.orders_sink FINAL
WHERE deleted = 0
  AND status IN ('PAID', 'SHIPPED', 'COMPLETED');
```

Doanh thu theo phút:

```sql
SELECT toStartOfMinute(updated_at) AS minute, sum(amount) AS revenue
FROM ecommerce_ods.orders_sink FINAL
WHERE deleted = 0
GROUP BY minute
ORDER BY minute;
```

Tỷ lệ hủy:

```sql
SELECT countIf(status = 'CANCELLED' OR deleted = 1) / count() AS cancel_rate
FROM ecommerce_ods.orders_sink FINAL;
```

## Lưu ý

- Khi kiểm thử correctness nên dùng `FINAL`.
- Query không có `FINAL` có thể thấy nhiều version trước khi ClickHouse merge.
- Với production lớn, cần tối ưu query để hạn chế dùng `FINAL` thường xuyên.
