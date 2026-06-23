# 21. Data Quality và Correctness Validation

## 1. Mục tiêu

Pipeline CDC không chỉ cần chạy được mà phải chứng minh dữ liệu đúng. Phần này dùng để kiểm tra PostgreSQL và ClickHouse có khớp nhau không.

## 2. Các nhóm kiểm tra

| Check | Mục tiêu |
|---|---|
| Count check | Số bản ghi khớp |
| Sum check | Tổng `amount` khớp |
| Status distribution | Số đơn theo trạng thái khớp |
| Deleted check | Soft delete khớp |
| Duplicate check | Không trùng ở latest state |
| Null check | Không null primary key |
| Latency check | Dữ liệu đến dưới 10 giây |

## 3. Count và sum

PostgreSQL:

```sql
SELECT COUNT(*) AS total_orders,
       COALESCE(SUM(amount), 0) AS total_amount
FROM orders
WHERE deleted = FALSE;
```

ClickHouse:

```sql
SELECT COUNT(*) AS total_orders,
       COALESCE(SUM(amount), 0) AS total_amount
FROM ecommerce_ods.orders_sink FINAL
WHERE deleted = 0;
```

## 4. Theo trạng thái

PostgreSQL:

```sql
SELECT status, COUNT(*)
FROM orders
WHERE deleted = FALSE
GROUP BY status
ORDER BY status;
```

ClickHouse:

```sql
SELECT status, COUNT(*)
FROM ecommerce_ods.orders_sink FINAL
WHERE deleted = 0
GROUP BY status
ORDER BY status;
```

## 5. Soft delete

PostgreSQL:

```sql
SELECT COUNT(*) FROM orders WHERE deleted = TRUE;
```

ClickHouse:

```sql
SELECT COUNT(*) FROM ecommerce_ods.orders_sink FINAL WHERE deleted = 1;
```

## 6. Duplicate latest state

```sql
SELECT order_id, COUNT(*)
FROM ecommerce_ods.orders_sink FINAL
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Kỳ vọng: không có dòng nào.

## 7. Test theo thao tác

### INSERT

```sql
INSERT INTO orders(order_id, customer_id, status, amount, deleted)
VALUES (910001, 1001, 'CREATED', 100000, FALSE);
```

Kỳ vọng ClickHouse có `order_id=910001`.

### UPDATE

```sql
UPDATE orders
SET status = 'PAID', updated_at = CURRENT_TIMESTAMP
WHERE order_id = 910001;
```

Kỳ vọng ClickHouse `status='PAID'`.

### SOFT DELETE

```sql
UPDATE orders
SET deleted = TRUE,
    status = 'CANCELLED',
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 910001;
```

Kỳ vọng ClickHouse `deleted=1`.

## 8. Script Python đề xuất

File:

```text
scripts/check_correctness.py
```

Output mong muốn:

```text
[OK] Count matched: 10000
[OK] Sum amount matched: 523456789.00
[OK] Status distribution matched
[OK] Deleted count matched
```

Nếu lỗi:

```text
[FAIL] Count mismatch: postgres=10000 clickhouse=9998
```

## 9. Bảng kết quả báo cáo

| Test case | PostgreSQL | ClickHouse | Kết quả |
|---|---:|---:|---|
| Count active orders | 10000 | 10000 | Pass |
| Sum amount | 523456789 | 523456789 | Pass |
| Deleted orders | 500 | 500 | Pass |
| CREATED orders | 3000 | 3000 | Pass |
| PAID orders | 4000 | 4000 | Pass |

## 10. Checklist

- [ ] Test INSERT.
- [ ] Test UPDATE.
- [ ] Test SOFT DELETE.
- [ ] Count check.
- [ ] Sum check.
- [ ] Status distribution.
- [ ] Deleted check.
- [ ] Duplicate check.
- [ ] Bảng kết quả.

## 11. Đoạn viết báo cáo

Để đánh giá correctness, hệ thống đối soát dữ liệu giữa PostgreSQL và ClickHouse theo nhiều tiêu chí gồm số lượng bản ghi, tổng `amount`, phân bố theo trạng thái, số bản ghi soft delete và kiểm tra duplicate theo `order_id`. Do ClickHouse sử dụng `ReplacingMergeTree`, các truy vấn kiểm chứng trạng thái mới nhất sử dụng `FINAL`. Kết quả đối soát giúp chứng minh pipeline không chỉ truyền dữ liệu thành công mà còn duy trì tính nhất quán giữa OLTP source và OLAP sink.
