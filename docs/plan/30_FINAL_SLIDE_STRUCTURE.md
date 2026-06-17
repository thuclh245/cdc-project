# 30. Cấu trúc slide báo cáo

## 1. Mục tiêu slide

Slide cần đi theo mạch:

```text
Bài toán → Kiến trúc → Triển khai → Kiểm thử → Kết quả → Mở rộng
```

Không nên đưa quá nhiều code. Slide nên dùng sơ đồ, bảng kết quả và nhận xét.

## 2. Số lượng đề xuất

12–15 slide.

## 3. Slide 1 — Title

Nội dung:

```text
Xây dựng luồng CDC đồng bộ và xử lý dữ liệu từ OLTP sang OLAP database với Flink CDC
```

Công nghệ: PostgreSQL, Flink CDC, ClickHouse, Docker Compose, HAProxy.

## 4. Slide 2 — Bối cảnh

Ý chính:

- E-commerce phát sinh giao dịch liên tục.
- PostgreSQL là OLTP source.
- Query phân tích trực tiếp trên PostgreSQL gây tải.
- Cần đồng bộ sang OLAP.

Thông điệp:

```text
Không nên dùng OLTP database làm nơi chạy phân tích nặng.
```

## 5. Slide 3 — Mục tiêu

- Xây dựng CDC pipeline PostgreSQL → ClickHouse.
- Flink CDC đọc `INSERT/UPDATE/DELETE`.
- PostgreSQL HA: 1 primary + 2 replica.
- ClickHouse phục vụ OLAP.
- Đo latency, throughput, correctness, fault tolerance.
- Latency mục tiêu < 10 giây.

## 6. Slide 4 — Công nghệ

| Thành phần | Vai trò |
|---|---|
| PostgreSQL | OLTP source |
| Flink CDC | Streaming CDC engine |
| ClickHouse | OLAP sink |
| HAProxy | Proxy mô phỏng production |
| Docker Compose | Multi-container deployment |

## 7. Slide 5 — Kiến trúc tổng thể

Sơ đồ:

```text
HAProxy → pg-primary → pg-replica-1/2
pg-primary → Flink CDC → ClickHouse
```

Thông điệp:

```text
Tách luồng giao dịch OLTP và luồng phân tích OLAP bằng CDC.
```

## 8. Slide 6 — PostgreSQL HA

- `pg-primary`: nhận ghi.
- `pg-replica-1`, `pg-replica-2`: standby.
- Replication bằng WAL streaming.
- Replica khởi tạo bằng `pg_basebackup`.
- Kiểm chứng bằng `pg_is_in_recovery()` và `pg_stat_replication`.

## 9. Slide 7 — Flink CDC

- Flink kết nối PostgreSQL primary.
- Đọc logical WAL.
- Bắt INSERT/UPDATE/DELETE.
- Ghi sang ClickHouse.
- Dockerfile riêng để thêm connector jar.

Câu nói dễ hiểu:

```text
Docker network giúp container nhìn thấy nhau, connector giúp Flink hiểu cách đọc PostgreSQL CDC và ghi ClickHouse.
```

## 10. Slide 8 — Thiết kế dữ liệu

Source:

```text
orders(order_id, customer_id, status, amount, created_at, updated_at, deleted)
```

Sink:

```text
orders_sink
ReplacingMergeTree(updated_at)
ORDER BY order_id
```

Ý nghĩa:

- `order_id`: idempotent key.
- `updated_at`: version/event-time.
- `deleted`: soft delete.

## 11. Slide 9 — Idempotent và ordering

- Flink có thể retry/restart.
- Sink cần chịu được ghi lặp.
- Dùng `order_id + updated_at`.
- Soft delete thay vì hard delete.
- Query latest bằng `FINAL`.

## 12. Slide 10 — Kiểm thử chức năng

| Test | Kỳ vọng |
|---|---|
| INSERT | ClickHouse có dòng mới |
| UPDATE | ClickHouse cập nhật status |
| SOFT DELETE | `deleted=1` |
| Replica | 2 replica streaming |
| Slot | Flink slot active |

## 13. Slide 11 — Đo kiểm

| Nhóm | Cách đo |
|---|---|
| Latency | PostgreSQL write → ClickHouse visible |
| Throughput | Rows/s |
| Correctness | Count/sum/status |
| Fault tolerance | Restart TaskManager |

## 14. Slide 12 — Kết quả thực nghiệm

Bảng mẫu:

| Chỉ số | Kết quả | Nhận xét |
|---|---:|---|
| Latency trung bình | Điền số | Đạt/chưa đạt |
| Throughput | Điền số | Ổn định/chưa |
| Correctness count | Pass | Khớp |
| Correctness sum | Pass | Khớp |
| Restart TaskManager | Pass | Không mất dữ liệu |

## 15. Slide 13 — Production-like readiness

Đã có:

- PostgreSQL HA.
- Flink CDC.
- Checkpoint volume.
- Idempotent sink.
- Soft delete.
- Fault tolerance test.

Còn thiếu:

- Prometheus/Grafana.
- Kafka.
- Security hardening.
- Schema evolution.
- Kubernetes.

## 16. Slide 14 — Hướng mở rộng

- Multi-table CDC.
- Event-time analytics.
- Kafka buffer.
- StarRocks comparison.
- Data quality automation.
- Schema evolution.
- Monitoring.

## 17. Slide 15 — Kết luận

- Đã xây dựng CDC pipeline end-to-end.
- PostgreSQL HA hoạt động.
- Flink CDC đọc được thay đổi.
- ClickHouse nhận dữ liệu phục vụ OLAP.
- Có định hướng mở rộng production-like.

Câu kết:

```text
CDC giúp tách tải phân tích khỏi hệ thống giao dịch và cung cấp dữ liệu gần thời gian thực cho OLAP.
```

## 18. Backup slides nên có

### CDC vs Batch ETL

| CDC | Batch ETL |
|---|---|
| Gần real-time | Theo lịch |
| Bắt thay đổi nhỏ | Quét dữ liệu lớn |
| Phù hợp dashboard realtime | Phù hợp báo cáo định kỳ |

### Physical vs Logical replication

| Physical replication | Logical replication |
|---|---|
| Dùng cho standby replica | Dùng cho CDC |
| Sao chép WAL ở mức vật lý | Phát thay đổi theo bảng |
| pg-primary → replica | pg-primary → Flink CDC |

### Checkpoint vs Savepoint

| Checkpoint | Savepoint |
|---|---|
| Tự động | Thủ công |
| Phục hồi lỗi | Nâng cấp/dừng job |
| Flink quản lý | Người vận hành quản lý |

## 19. Checklist slide

- [ ] Có bài toán.
- [ ] Có kiến trúc.
- [ ] Có công nghệ.
- [ ] Có schema.
- [ ] Có PostgreSQL HA.
- [ ] Có Flink CDC.
- [ ] Có idempotent.
- [ ] Có test.
- [ ] Có kết quả.
- [ ] Có production readiness.
- [ ] Có hướng mở rộng.
