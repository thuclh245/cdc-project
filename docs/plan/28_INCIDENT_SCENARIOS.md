# 28. Incident Scenarios và cách xử lý

## 1. Mục tiêu

Tài liệu này liệt kê các kịch bản lỗi quan trọng của pipeline CDC và cách phát hiện, phân tích, xử lý.

## 2. Flink job FAILED

### Dấu hiệu

- Flink UI không còn RUNNING.
- `flink list` không thấy job.
- Log có exception.

### Nguyên nhân

- Thiếu connector jar.
- Sai hostname PostgreSQL/ClickHouse.
- Sai user/password.
- Schema mismatch.
- Sink timeout.

### Xử lý

```powershell
docker logs flink-jobmanager
docker logs flink-taskmanager
docker exec -it flink-jobmanager ls /opt/flink/lib
```

Sửa lỗi rồi submit lại job.

## 3. Replication slot inactive

Query:

```sql
SELECT slot_name, active
FROM pg_replication_slots;
```

Nếu `active = f`:

- Flink job chưa chạy hoặc failed.
- Sai slot name.
- Kết nối bị lỗi.

Xử lý:

- Kiểm tra Flink job.
- Kiểm tra logs.
- Chạy lại job.
- Không vội drop slot nếu chưa chắc chắn.

## 4. WAL retained tăng cao

Query:

```sql
SELECT
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

Nguyên nhân:

- Flink không đọc tiếp WAL.
- Sink chậm.
- Job bị backpressure.

Hậu quả: PostgreSQL có thể đầy disk.

Xử lý:

- Khôi phục Flink job.
- Kiểm tra ClickHouse.
- Nếu chắc chắn không cần slot nữa mới drop slot.

## 5. Replica không streaming

Dấu hiệu:

```sql
SELECT application_name, state
FROM pg_stat_replication;
```

Không đủ 2 dòng hoặc `state != streaming`.

Nguyên nhân:

- Replica chưa chạy.
- `pg_basebackup` lỗi.
- Sai user replication.
- Sai `pg_hba.conf`.
- Volume replica đã init sai.

Xử lý:

- Kiểm tra log replica.
- Kiểm tra `pg_is_in_recovery()`.
- Nếu replica không phải standby, rebuild replica volume.

## 6. ClickHouse không nhận dữ liệu

Dấu hiệu:

```sql
SELECT count() FROM ecommerce_ods.orders_sink FINAL;
```

Không tăng sau khi insert PostgreSQL.

Xử lý theo thứ tự:

1. Flink job RUNNING?
2. Slot active?
3. TaskManager log có lỗi sink?
4. ClickHouse table đúng tên?
5. JDBC URL đúng container name?

## 7. Dữ liệu bị trùng

Kiểm tra vật lý:

```sql
SELECT order_id, COUNT(*)
FROM ecommerce_ods.orders_sink
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Kiểm tra logic:

```sql
SELECT order_id, COUNT(*)
FROM ecommerce_ods.orders_sink FINAL
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Nếu chỉ trùng vật lý nhưng query `FINAL` đúng thì không phải lỗi logic nghiêm trọng với `ReplacingMergeTree`.

## 8. Dữ liệu sai sau UPDATE

Nguyên nhân thường gặp:

- `updated_at` không đổi.
- Version không tăng.
- Query không dùng `FINAL`.
- Event đến sai thứ tự.

Cách xử lý:

- Update luôn kèm `updated_at = CURRENT_TIMESTAMP`.
- Cân nhắc thêm `event_version`.
- Query latest state bằng `FINAL`.

## 9. Soft delete không đúng

Dấu hiệu: PostgreSQL `deleted=true` nhưng ClickHouse vẫn active.

Xử lý:

- Kiểm tra Flink job.
- Kiểm tra row theo `order_id` trong ClickHouse.
- Đảm bảo update delete có `updated_at` mới.
- Query `FINAL` và lọc `deleted=0`.

## 10. Schema mismatch

Dấu hiệu: job fail sau khi thêm/sửa cột.

Xử lý:

- Dừng job bằng savepoint nếu có.
- Cập nhật ClickHouse schema.
- Cập nhật Flink SQL.
- Chạy lại job.
- Chạy correctness check.

## 11. Latency vượt 10 giây

Nguyên nhân:

- Docker local thiếu tài nguyên.
- ClickHouse ghi chậm.
- Flink backpressure.
- Checkpoint quá dày.
- Source sinh dữ liệu quá nhanh.

Xử lý:

- Giảm tốc độ sinh dữ liệu.
- Tăng tài nguyên Docker.
- Tăng checkpoint interval.
- Kiểm tra Flink UI.
- Kiểm tra ClickHouse logs.

## 12. Bảng tổng hợp

| Incident | Mức độ | Phát hiện | Xử lý chính |
|---|---|---|---|
| Flink FAILED | Cao | Flink UI/log | Sửa config, restart job |
| Slot inactive | Cao | `pg_replication_slots` | Khôi phục job |
| WAL retained cao | Cao | retained WAL query | Khôi phục Flink/sink |
| Replica không streaming | Trung bình | `pg_stat_replication` | Rebuild replica |
| ClickHouse không nhận | Cao | count không tăng | Check job/sink |
| Duplicate | Trung bình | duplicate query | Dùng `FINAL`, version |
| Schema mismatch | Cao | job fail | Migration + savepoint |
| Latency cao | Trung bình | latency test | Tuning/resource |

## 13. Đoạn viết báo cáo

Dự án phân tích các kịch bản sự cố có thể xảy ra trong pipeline CDC như Flink job failed, replication slot inactive, WAL retained tăng cao, replica không streaming, ClickHouse không nhận dữ liệu, duplicate, schema mismatch và latency vượt ngưỡng. Với mỗi sự cố, tài liệu xác định dấu hiệu phát hiện, nguyên nhân và cách xử lý. Điều này giúp hệ thống có tính production-like hơn vì không chỉ chứng minh chạy được mà còn xem xét khả năng vận hành và phục hồi khi xảy ra lỗi.
