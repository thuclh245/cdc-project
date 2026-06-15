# Week 1 - Flink CDC PostgreSQL to ClickHouse

## Problem

Hệ thống OLTP PostgreSQL lưu dữ liệu giao dịch. Nếu truy vấn phân tích trực tiếp trên PostgreSQL, hệ thống giao dịch có thể bị ảnh hưởng. Do đó cần đồng bộ dữ liệu sang ClickHouse, một hệ OLAP phù hợp cho truy vấn phân tích.

## Goal

Xây dựng pipeline CDC từ PostgreSQL sang ClickHouse bằng Apache Flink CDC.

## Scope Week 1

- Dựng môi trường Docker Compose.
- Cấu hình PostgreSQL logical replication.
- Chạy Flink cluster standalone.
- Dựng ClickHouse.
- Test CDC với bảng orders.
- Kiểm tra INSERT, UPDATE, DELETE.
- Chuẩn bị script sinh dữ liệu lớn.

## Non-goal

- Chưa triển khai Kubernetes.
- Chưa tối ưu production.
- Chưa làm nhiều bảng nghiệp vụ phức tạp.

## Lý thuyết cần nắm trong ngày 1

Bạn cần hiểu 5 khái niệm:

| Khái niệm | Hiểu đơn giản |
|---|---|
| OLTP | Database phục vụ giao dịch: đơn hàng, thanh toán, cập nhật trạng thái |
| OLAP | Database phục vụ phân tích: thống kê, dashboard, tổng hợp |
| CDC | Change Data Capture, bắt thay đổi dữ liệu từ source |
| WAL | Write-Ahead Log của PostgreSQL, nơi ghi lại thay đổi trước khi commit bền vững |
| Flink CDC | Công cụ đọc thay đổi từ source database và đẩy sang sink |

Trong *Designing Data-Intensive Applications*, phần stream processing có các chủ đề rất sát với đề tài của bạn như:
- Databases and streams
- Keeping systems in sync
- Change data capture
- Fault tolerance
