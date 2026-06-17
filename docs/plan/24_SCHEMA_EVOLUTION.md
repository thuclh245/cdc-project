# 24. Schema Evolution trong CDC Pipeline

## 1. Mục tiêu

Production database thường thay đổi schema. Pipeline CDC cần có quy trình để tránh lỗi khi PostgreSQL, Flink SQL và ClickHouse không còn khớp schema.

## 2. Ví dụ thay đổi schema

Ban đầu `orders` có:

```text
order_id, customer_id, status, amount, created_at, updated_at, deleted
```

Sau đó thêm:

```text
discount_amount
payment_method
shipping_fee
```

Nếu chỉ thêm ở PostgreSQL mà không cập nhật Flink/ClickHouse, job có thể fail hoặc ghi thiếu dữ liệu.

## 3. Phân loại rủi ro

| Thay đổi | Rủi ro | Ví dụ |
|---|---:|---|
| Thêm cột nullable | Thấp | `discount_amount` |
| Thêm cột có default | Thấp-trung bình | `payment_method DEFAULT 'COD'` |
| Đổi kiểu dữ liệu | Cao | `amount NUMERIC` → `TEXT` |
| Đổi tên cột | Cao | `status` → `order_status` |
| Xóa cột | Cao | bỏ `deleted` |
| Đổi primary key | Rất cao | đổi `order_id` |

## 4. Quy trình thay đổi an toàn

### Bước 1: Phân tích

- Cột mới có nullable không?
- Có default không?
- Flink source/sink có cần cập nhật không?
- ClickHouse có cần migration không?
- Query báo cáo có bị ảnh hưởng không?

### Bước 2: Migration PostgreSQL

```sql
ALTER TABLE orders
ADD COLUMN discount_amount NUMERIC(12,2) DEFAULT 0;
```

### Bước 3: Migration ClickHouse

```sql
ALTER TABLE cdc_demo.orders_sink
ADD COLUMN discount_amount Decimal(12,2) DEFAULT 0;
```

### Bước 4: Cập nhật Flink SQL

Thêm cột vào source và sink table definition.

### Bước 5: Dừng job bằng savepoint

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink stop --savepointPath file:///opt/flink/checkpoints/savepoints <JOB_ID>
```

### Bước 6: Chạy job mới

Submit lại job với schema mới.

### Bước 7: Kiểm tra

```sql
SELECT discount_amount
FROM cdc_demo.orders_sink FINAL
ORDER BY updated_at DESC
LIMIT 10;
```

## 5. Nguyên tắc backward-compatible

```text
Thêm trước, dùng sau.
```

Quy trình an toàn:

1. Thêm cột nullable/default.
2. Thêm cột vào sink.
3. Cập nhật Flink job.
4. Kiểm thử.
5. Sau khi ổn định mới dùng trong dashboard/report.

Không nên:

- Đổi tên cột đột ngột.
- Xóa cột đang dùng.
- Đổi kiểu dữ liệu khi chưa test.
- Đổi primary key.

## 6. Version hóa schema

Tổ chức thư mục:

```text
schemas/
  orders_v1.sql
  orders_v2_add_discount.sql
  orders_v3_add_payment_method.sql
```

Mỗi migration cần có:

- Lý do thay đổi.
- SQL PostgreSQL.
- SQL ClickHouse.
- Thay đổi Flink SQL.
- Rollback plan.

## 7. Rollback

Nếu deploy lỗi:

1. Dừng job.
2. Restore từ savepoint nếu tương thích.
3. Rollback Flink SQL.
4. Giữ cột mới nếu không gây hại.
5. Ghi incident log.

## 8. Thêm bảng mới

```sql
CREATE TABLE payments (...);
ALTER TABLE payments REPLICA IDENTITY FULL;
ALTER PUBLICATION ecommerce_pub ADD TABLE payments;
```

Sau đó:

- Tạo Flink source table.
- Tạo ClickHouse sink table.
- Chạy job.
- Kiểm tra data quality.

## 9. Checklist

- [ ] Có quy trình migration.
- [ ] Có migration PostgreSQL.
- [ ] Có migration ClickHouse.
- [ ] Có cập nhật Flink SQL.
- [ ] Có savepoint trước deploy.
- [ ] Có test sau deploy.
- [ ] Có rollback plan.

## 10. Đoạn viết báo cáo

Trong môi trường production, schema dữ liệu nguồn có thể thay đổi theo thời gian. Vì vậy pipeline CDC cần có quy trình schema evolution để đảm bảo PostgreSQL, Flink SQL và ClickHouse luôn tương thích. Các thay đổi an toàn như thêm cột nullable hoặc có default value có thể thực hiện theo quy trình migration có kiểm soát. Trước khi nâng cấp job, savepoint được dùng để lưu trạng thái, giúp khôi phục nếu deploy lỗi. Các thay đổi rủi ro cao như đổi kiểu dữ liệu, đổi tên cột hoặc đổi primary key cần được kiểm thử kỹ và có kế hoạch rollback.
