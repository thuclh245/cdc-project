# 13. Hướng mở rộng sau này


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Mở rộng dữ liệu

V1 chỉ dùng `orders`. Sau này có thể thêm:

```text
customers
products
order_items
payments
```

Lợi ích:

- Gần với hệ thống e-commerce thật hơn.
- Hỗ trợ truy vấn theo khách hàng, sản phẩm, danh mục.
- Có thể xây dựng data mart/star schema.

## Multi-table CDC

Đồng bộ nhiều bảng từ PostgreSQL sang ClickHouse. Cần xử lý:

- Nhiều source table trong Flink.
- Join/denormalization.
- Consistency giữa các bảng.
- Schema evolution.

## Kafka trung gian

Kiến trúc mở rộng:

```text
PostgreSQL → Flink CDC/Debezium → Kafka → Flink → ClickHouse
```

Lợi ích:

- Buffer event.
- Replay dữ liệu.
- Nhiều consumer.
- Tách source và sink.

## So sánh StarRocks

Có thể triển khai song song:

```text
PostgreSQL → Flink CDC → ClickHouse
PostgreSQL → Flink CDC → StarRocks
```

So sánh theo latency, throughput, khả năng update/delete, SQL analytics.

## Monitoring

Thêm:

- Prometheus.
- Grafana.
- Alertmanager.

Metrics cần theo dõi:

- Flink job status.
- Checkpoint duration/failure.
- Backpressure.
- PostgreSQL replication lag.
- Replication slot lag.
- ClickHouse insert/query latency.

## HA nâng cao

- Flink HA JobManager.
- PostgreSQL automatic failover bằng Patroni/repmgr.
- ClickHouse cluster.
- Durable checkpoint storage.

## Data quality

Thêm các check:

- Count source/sink.
- Sum amount source/sink.
- Không null primary key.
- `updated_at >= created_at`.
- Status hợp lệ.

## Dashboard BI

Kết nối ClickHouse với:

- Grafana.
- Superset.
- Metabase.
- Power BI.

Dashboard gợi ý:

- Doanh thu theo thời gian.
- Số đơn theo trạng thái.
- Tỷ lệ hủy đơn.
- Latency pipeline.
