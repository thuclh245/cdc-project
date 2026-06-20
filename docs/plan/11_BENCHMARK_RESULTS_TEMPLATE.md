# 11. Template kết quả đo kiểm


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Môi trường

| Thành phần | Thông tin |
|---|---|
| OS |  |
| CPU/RAM |  |
| Docker version |  |
| PostgreSQL | 16 |
| Flink | 1.18.1 |
| ClickHouse |  |

## PostgreSQL HA

| Kiểm tra | Mong đợi | Thực tế | Đạt |
|---|---|---|---|
| Primary recovery | `f` |  |  |
| Replica 1 recovery | `t` |  |  |
| Replica 2 recovery | `t` |  |  |
| `pg_stat_replication` | 2 streaming |  |  |

## Flink CDC

| Kiểm tra | Mong đợi | Thực tế | Đạt |
|---|---|---|---|
| Job status | RUNNING |  |  |
| Slot active | `t` |  |  |
| INSERT | Đồng bộ |  |  |
| UPDATE | Đồng bộ |  |  |
| Soft delete | Đồng bộ |  |  |

## Latency

| Lần | Loại thao tác | order_id | Latency giây | Đạt < 10s |
|---:|---|---:|---:|---|
| 1 | INSERT |  |  |  |
| 2 | UPDATE |  |  |  |
| 3 | SOFT DELETE |  |  |  |

## Throughput

| Batch size | Tổng thời gian giây | Rows/s | Ghi chú |
|---:|---:|---:|---|
| 1.000 |  |  |  |
| 10.000 |  |  |  |
| 50.000 |  |  |  |

## Correctness

| Metric | PostgreSQL | ClickHouse | Khớp |
|---|---:|---:|---|
| Count active |  |  |  |
| Sum revenue |  |  |  |
| Count cancelled |  |  |  |

## Fault tolerance

| Bước | Kết quả |
|---|---|
| Job trước restart |  |
| Restart TaskManager |  |
| Job phục hồi |  |
| Insert sau restart |  |
| ClickHouse nhận dữ liệu |  |

## Nhận xét mẫu

Kết quả thực nghiệm cho thấy pipeline CDC hoạt động đúng với các thao tác INSERT, UPDATE và soft delete. PostgreSQL primary-replica hoạt động ở trạng thái streaming, Flink CDC đọc được thay đổi từ WAL thông qua logical replication slot, và ClickHouse nhận dữ liệu để phục vụ truy vấn phân tích. Latency đạt mục tiêu dưới 10 giây trong môi trường Docker local.
