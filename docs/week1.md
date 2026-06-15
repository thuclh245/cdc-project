# Tìm hiểu về Apache Flink,

## Flink

# Problem

Hệ thống OLTP PostgreSQL lưu trữ dữ liệu giao dịch. Nếu truy vấn trực tiếp trên PostgreSQL hệ giao dịch có thể bị ảnh hưởng.
-> Do đo cần đồng bộ dữ liệu sang ClickHouse, một hệ thống OLAP để phục vụ cho việc phân tích dữ liệu.

## Goal

Xây dựng pipeline từ PostgreSQL sang ClickHouse bằng Apache Flink.

## Todolist

- [] Dựng môi trường Docker Compose
- [] Setup PostgreSQL logical replication
- [] Chạy flink cluster standalon
- [] Test CDC với bảng orders
- [] Kiểm tra INSERT, UPDATE, DELETE
- [] Dựng ClinkHouse
- [] Chuẩn bị script sỉnh dữ liệu
