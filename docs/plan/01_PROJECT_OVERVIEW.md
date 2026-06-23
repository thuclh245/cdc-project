# 01. Tổng quan dự án


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Tên đề tài

**Xây dựng luồng CDC đồng bộ dữ liệu đơn hàng từ PostgreSQL OLTP sang ClickHouse OLAP bằng Apache Flink CDC**.

## Mục tiêu

Dự án xây dựng pipeline CDC gần thời gian thực để đồng bộ dữ liệu từ PostgreSQL sang ClickHouse. PostgreSQL là hệ thống OLTP, Flink CDC là engine đọc thay đổi, ClickHouse là OLAP sink phục vụ phân tích.

## Phạm vi v1

- Dữ liệu nghiệp vụ: đơn hàng thương mại điện tử.
- Bảng chính: `orders`.
- PostgreSQL: 1 primary + 2 replica.
- Flink CDC đọc thay đổi từ PostgreSQL primary.
- ClickHouse lưu dữ liệu vào `ecommerce_ods.orders_sink`.
- Hỗ trợ thao tác `INSERT`, `UPDATE`, `DELETE` theo hướng soft delete.
- Đo kiểm: latency, throughput, correctness, fault tolerance.

## Công nghệ

| Thành phần | Công nghệ |
|---|---|
| OLTP source | PostgreSQL 16 |
| CDC engine | Apache Flink 1.18.1 + Flink CDC |
| OLAP sink | ClickHouse |
| Proxy | HAProxy |
| Deployment | Docker Compose |
| State | Flink checkpoint/savepoint |

## Kết quả đầu ra

- Docker Compose chạy được toàn hệ thống.
- PostgreSQL primary-replica hoạt động đúng.
- Flink CDC job `RUNNING`.
- ClickHouse nhận đúng dữ liệu sau `INSERT`, `UPDATE`, soft delete.
- Có bảng số liệu đo kiểm.
- Có báo cáo chi tiết và slide trình bày.
