# 14. Hướng dẫn viết báo cáo và slide


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Cấu trúc báo cáo đề xuất

1. Giới thiệu đề tài.
2. Bài toán đơn vị và sự cần thiết nghiên cứu.
3. Cơ sở lý thuyết: OLTP, OLAP, CDC, WAL, Flink, ClickHouse.
4. Phân tích lựa chọn dữ liệu.
5. Kiến trúc hệ thống.
6. Thiết kế dữ liệu.
7. Triển khai Docker Compose.
8. PostgreSQL primary-replica.
9. Flink CDC job.
10. ClickHouse sink.
11. Kiểm thử và đánh giá.
12. Production-like assessment.
13. Hạn chế và hướng phát triển.
14. Kết luận.

## Dàn ý slide

| Slide | Nội dung |
|---:|---|
| 1 | Tiêu đề đề tài |
| 2 | Bài toán và động lực |
| 3 | Mục tiêu hệ thống |
| 4 | Công nghệ sử dụng |
| 5 | Kiến trúc tổng thể |
| 6 | Thiết kế bảng `orders` |
| 7 | PostgreSQL HA |
| 8 | Flink CDC |
| 9 | ClickHouse sink |
| 10 | Kiểm thử INSERT/UPDATE/DELETE |
| 11 | Kết quả latency/throughput/correctness |
| 12 | Fault tolerance |
| 13 | Production-like và hạn chế |
| 14 | Hướng mở rộng |
| 15 | Kết luận |

## Ảnh cần chụp

- `docker ps`.
- Flink Web UI job `RUNNING`.
- `pg_is_in_recovery()` trên primary và replica.
- `pg_stat_replication` có 2 dòng streaming.
- `pg_replication_slots` active.
- ClickHouse nhận dữ liệu sau INSERT.
- ClickHouse nhận dữ liệu sau UPDATE.
- ClickHouse nhận soft delete.
- Bảng kết quả benchmark.

## Câu mở đầu thuyết trình

Đề tài của nhóm em xây dựng pipeline CDC đồng bộ dữ liệu đơn hàng từ PostgreSQL sang ClickHouse bằng Apache Flink CDC. PostgreSQL đóng vai trò hệ thống OLTP lưu giao dịch, Flink CDC đọc thay đổi từ WAL, còn ClickHouse đóng vai trò OLAP sink phục vụ truy vấn phân tích gần thời gian thực.

## Câu kết luận

Kết quả triển khai cho thấy pipeline có thể đồng bộ dữ liệu từ PostgreSQL sang ClickHouse sau các thao tác INSERT, UPDATE và soft delete. Hệ thống cũng kiểm thử được PostgreSQL replication, Flink CDC job, correctness, latency và fault tolerance cơ bản. Đây là nền tảng production-like để mở rộng thêm Kafka, monitoring, multi-table CDC và dashboard BI.
