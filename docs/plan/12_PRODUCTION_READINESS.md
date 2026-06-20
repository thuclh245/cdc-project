# 12. Đánh giá production-like


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Những điểm đã gần production

| Nhóm | Đã có | Ý nghĩa |
|---|---|---|
| Source HA | PostgreSQL primary + 2 replica | Mô phỏng HA tầng OLTP |
| CDC | Flink CDC đọc WAL | Không dùng batch polling |
| OLAP | ClickHouse | Tách tải phân tích |
| Checkpoint | Flink checkpoint volume | Recovery cơ bản |
| Savepoint | Có kế hoạch/lệnh | Dừng/nâng cấp job an toàn |
| Idempotent | `order_id + updated_at` | Giảm sai khi xử lý lặp |
| Delete | Soft delete | Phù hợp phân tích |
| Proxy | HAProxy | Mô phỏng lớp truy cập DB |
| Benchmark | Latency/throughput/correctness | Có đánh giá định lượng |

## Những điểm chưa phải production thật

| Nhóm | Hiện trạng | Cần bổ sung production thật |
|---|---|---|
| Deployment | Docker Compose local | Kubernetes/VM cluster |
| Monitoring | Flink UI, logs, SQL | Prometheus/Grafana/Alertmanager |
| Secret | Password trong compose | Secret manager |
| Flink HA | Chưa HA JobManager đầy đủ | HA mode |
| Checkpoint storage | Docker volume | S3/GCS/HDFS/NFS |
| ClickHouse | Single node | Cluster/replication |
| PostgreSQL failover | Chưa tự động promote | Patroni/repmgr/pg_auto_failover |
| Schema evolution | Chưa xử lý sâu | Quy trình migration |

## Rủi ro và giảm thiểu

| Rủi ro | Tác động | Giảm thiểu |
|---|---|---|
| Flink dừng lâu | WAL phình to do slot giữ WAL | Monitor slot lag |
| ClickHouse chậm | Backpressure | Theo dõi Flink UI, tăng tài nguyên |
| Dữ liệu ghi lặp | Sai phân tích | Idempotent sink |
| Event lệch thứ tự | Trạng thái cuối sai | Dùng `updated_at`/version |
| Checkpoint lỗi | Recovery khó | Dùng storage bền vững |

## Cách diễn đạt trong báo cáo

Hệ thống hiện tại là mô hình **production-like lab deployment**. Dự án đã mô phỏng các thành phần quan trọng của pipeline CDC production như PostgreSQL HA, Flink CDC, ClickHouse OLAP sink, checkpoint, savepoint, idempotent sink và fault tolerance test. Tuy nhiên, để triển khai production thật cần bổ sung monitoring tập trung, secret management, Flink HA, ClickHouse cluster và failover tự động cho PostgreSQL.
