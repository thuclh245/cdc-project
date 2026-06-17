# Bộ tài liệu mở rộng toàn diện cho dự án CDC OLTP → OLAP

## Mục tiêu

Bộ tài liệu này mở rộng dự án từ mức **CDC demo chạy được** sang mức **production-like**: có nhiều bảng, xử lý event-time, kiểm soát idempotent/order, checkpoint/savepoint, data quality, monitoring, incident runbook và lộ trình production.

Phiên bản v1 đã chốt:

| Hạng mục | Lựa chọn |
|---|---|
| Domain | E-commerce order processing |
| OLTP source | PostgreSQL |
| CDC engine | Apache Flink CDC |
| OLAP sink | ClickHouse |
| PostgreSQL HA | 1 primary + 2 replica |
| Bảng v1 | `orders` |
| Delete strategy | Soft delete |
| Sink strategy | Latest state trong ClickHouse |
| Latency target | < 10 giây trong lab |
| Deployment | Docker Compose + HAProxy |

## Danh sách file mở rộng

| File | Vai trò |
|---|---|
| `16_MULTI_TABLE_CDC.md` | Mở rộng từ `orders` sang nhiều bảng nghiệp vụ |
| `17_EVENT_TIME_PROCESSING.md` | Thiết kế xử lý event-time và window metrics |
| `18_IDEMPOTENCY_AND_ORDERING.md` | Đảm bảo idempotent và ordering |
| `19_CHECKPOINT_SAVEPOINT_HA.md` | Checkpoint, savepoint, recovery |
| `20_MONITORING_OBSERVABILITY.md` | Monitoring lab và production |
| `21_DATA_QUALITY_VALIDATION.md` | Đối soát dữ liệu nguồn-đích |
| `22_KAFKA_EXTENSION.md` | Kafka làm buffer/event bus |
| `23_STARROCKS_COMPARISON.md` | So sánh ClickHouse và StarRocks |
| `24_SCHEMA_EVOLUTION.md` | Quy trình thay đổi schema |
| `25_SECURITY_AND_ACCESS_CONTROL.md` | Bảo mật và phân quyền |
| `26_PRODUCTION_DEPLOYMENT_ROADMAP.md` | Lộ trình production |
| `27_OPERATIONS_RUNBOOK.md` | Runbook vận hành |
| `28_INCIDENT_SCENARIOS.md` | Kịch bản lỗi và cách xử lý |
| `29_FINAL_REPORT_STRUCTURE.md` | Khung báo cáo cuối |
| `30_FINAL_SLIDE_STRUCTURE.md` | Khung slide cuối |

## Cách triển khai theo mức ưu tiên

### P0 — Bắt buộc để hoàn thành đề tài

- PostgreSQL HA chạy đúng.
- Flink CDC đọc `INSERT/UPDATE/DELETE` từ PostgreSQL.
- ClickHouse nhận dữ liệu.
- Query kiểm tra `orders_sink FINAL`.
- Có ảnh/log minh chứng.

### P1 — Rất nên làm để bài có chiều sâu

- Soft delete chuẩn với `deleted`.
- ClickHouse dùng `ReplacingMergeTree(updated_at)`.
- Đo latency/throughput/correctness/fault tolerance.
- Restart Flink TaskManager và kiểm tra không mất dữ liệu.
- Tạo runbook vận hành.

### P2 — Mở rộng nếu còn thời gian

- Event-time aggregation.
- Data quality automation.
- Savepoint demo.
- Multi-table CDC.

### P3 — Future work production

- Kafka.
- StarRocks.
- Prometheus/Grafana.
- Schema evolution nâng cao.
- Kubernetes.
- Security hardening.

## Câu kết luận dùng trong báo cáo

Dự án không chỉ triển khai pipeline CDC từ PostgreSQL sang ClickHouse, mà còn định hướng mở rộng theo kiến trúc production-like. Các nội dung mở rộng gồm multi-table CDC, event-time processing, idempotency, ordering, checkpoint/savepoint, monitoring, data quality validation, schema evolution, security và lộ trình production. Cách tổ chức này giúp phân biệt rõ phần đã triển khai trong phạm vi thực nghiệm và phần có thể phát triển tiếp trong môi trường thực tế.
