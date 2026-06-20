# 04. Kiến trúc hệ thống


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Thành phần chính

| Thành phần | Vai trò |
|---|---|
| `pg-primary` | PostgreSQL primary, nhận ghi dữ liệu |
| `pg-replica-1`, `pg-replica-2` | Standby replicas, nhận WAL từ primary |
| `pg-haproxy` | Proxy mô phỏng lớp truy cập PostgreSQL |
| `flink-jobmanager` | Điều phối Flink job |
| `flink-taskmanager` | Thực thi CDC task |
| `clickhouse-sink` | OLAP sink lưu `orders_sink` |
| `cdc-network` | Docker network cho các container |

## Sơ đồ Mermaid

```mermaid
flowchart TB
    subgraph NET["Docker Compose Network: cdc-network"]
        subgraph PGHA["PostgreSQL HA Cluster"]
            H["pg-haproxy<br/>ports: 15432,15433"]
            P["pg-primary<br/>PostgreSQL Primary<br/>host port: 5433<br/>table: orders"]
            R1["pg-replica-1<br/>Standby<br/>host port: 5434"]
            R2["pg-replica-2<br/>Standby<br/>host port: 5435"]
            H -->|route database connection| P
            P -->|physical WAL streaming| R1
            P -->|physical WAL streaming| R2
        end
        subgraph FLINK["Apache Flink Cluster"]
            JM["flink-jobmanager<br/>Web UI: 8081"]
            TM["flink-taskmanager"]
            JM <--> TM
        end
        CH["clickhouse-sink<br/>ClickHouse<br/>8123/9000<br/>cdc_demo.orders_sink"]
        P -->|logical WAL / pgoutput<br/>INSERT, UPDATE, DELETE| JM
        JM -->|CDC change events| CH
    end
```

## Luồng dữ liệu

1. Dữ liệu đơn hàng được ghi vào `pg-primary.orders`.
2. PostgreSQL ghi thay đổi vào WAL.
3. Hai replica nhận physical WAL streaming từ primary.
4. Flink CDC đọc logical WAL trên primary thông qua publication/slot.
5. Flink ghi event sang ClickHouse.
6. ClickHouse phục vụ truy vấn phân tích.

## Lưu ý kiến trúc

- Flink CDC đọc từ `pg-primary`, không đọc từ replica.
- HAProxy mô phỏng proxy database, không thay thế cơ chế CDC.
- Trong Docker network, các container gọi nhau bằng tên service/container, ví dụ `pg-primary:5432`, `clickhouse-sink:8123`.
