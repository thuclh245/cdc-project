# 00. Mục lục bộ tài liệu dự án CDC


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Danh sách file

| File | Vai trò |
|---|---|
| `01_PROJECT_OVERVIEW.md` | Tổng quan đề tài, mục tiêu, phạm vi |
| `02_BUSINESS_PROBLEM.md` | Bài toán đơn vị và lý do cần nghiên cứu |
| `03_DATA_SOURCE_SELECTION.md` | Chọn nguồn dữ liệu và lý do chọn e-commerce orders |
| `04_ARCHITECTURE.md` | Kiến trúc tổng thể và sơ đồ Mermaid |
| `05_DATA_MODEL.md` | Thiết kế bảng PostgreSQL và ClickHouse |
| `06_DEPLOYMENT_DOCKER.md` | Mô tả Docker Compose, service, port, volume |
| `07_POSTGRES_HA.md` | PostgreSQL primary-replica, WAL, pg_basebackup |
| `08_FLINK_CDC_JOB.md` | Flink CDC, connector, checkpoint, savepoint |
| `09_CLICKHOUSE_SINK.md` | ClickHouse sink, idempotent, soft delete |
| `10_TEST_PLAN.md` | Test plan từ A-Z |
| `11_BENCHMARK_RESULTS_TEMPLATE.md` | Template điền số liệu đo kiểm |
| `12_PRODUCTION_READINESS.md` | Đánh giá production-like |
| `13_FUTURE_EXTENSIONS.md` | Hướng mở rộng sau này |
| `14_REPORT_SLIDE_GUIDE.md` | Hướng dẫn viết báo cáo và slide |
| `15_COMMANDS_CHEATSHEET.md` | Bộ lệnh nhanh demo/kiểm thử |

## Thứ tự làm việc đề xuất

1. Đọc `01`, `02`, `03` để chốt bài toán.
2. Dùng `04`, `05` để vẽ kiến trúc và thiết kế dữ liệu.
3. Dùng `06`, `07`, `08`, `09` để triển khai.
4. Chạy test theo `10`, ghi kết quả vào `11`.
5. Dùng `12`, `13`, `14` để hoàn thiện báo cáo và slide.
