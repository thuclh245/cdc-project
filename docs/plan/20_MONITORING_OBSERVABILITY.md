# 20. Monitoring và Observability

## 1. Mục tiêu

Một pipeline CDC production-like cần trả lời được:

```text
Job có đang chạy không?
PostgreSQL replica có streaming không?
Flink có đọc WAL không?
ClickHouse có nhận dữ liệu không?
Latency có vượt ngưỡng không?
Có nguy cơ đầy WAL không?
```

## 2. Monitoring trong phạm vi lab

| Thành phần | Cách kiểm tra |
|---|---|
| Docker | `docker ps`, `docker logs` |
| PostgreSQL primary | `pg_is_in_recovery()` |
| PostgreSQL replica | `pg_is_in_recovery()`, `pg_stat_wal_receiver` |
| Replication | `pg_stat_replication` |
| CDC slot | `pg_replication_slots` |
| Flink | Flink UI `localhost:8081`, `flink list` |
| ClickHouse | `SELECT ... FROM orders_sink FINAL` |
| HAProxy | `docker logs pg-haproxy` |

## 3. PostgreSQL checks

Primary:

```sql
SELECT pg_is_in_recovery();
SHOW wal_level;
SELECT * FROM pg_publication;
```

Replica:

```sql
SELECT pg_is_in_recovery();
SELECT COUNT(*) FROM orders;
```

Replication:

```sql
SELECT application_name, client_addr, state, sync_state
FROM pg_stat_replication;
```

CDC slot:

```sql
SELECT slot_name, plugin, slot_type, active
FROM pg_replication_slots;
```

WAL retained:

```sql
SELECT
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

## 4. Flink monitoring

Command:

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink list
```

Logs:

```powershell
docker logs flink-jobmanager
docker logs flink-taskmanager
```

Flink UI cần chụp:

- Job status RUNNING.
- Job graph.
- Records in/out.
- Checkpoints.
- TaskManager status.

## 5. ClickHouse monitoring

```sql
SELECT count() FROM ecommerce_ods.orders_sink FINAL;

SELECT *
FROM ecommerce_ods.orders_sink FINAL
ORDER BY updated_at DESC
LIMIT 10;
```

Duplicate vật lý:

```sql
SELECT order_id, count()
FROM ecommerce_ods.orders_sink
GROUP BY order_id
HAVING count() > 1
ORDER BY count() DESC
LIMIT 10;
```

Lưu ý: với `ReplacingMergeTree`, duplicate vật lý không nhất thiết là sai nếu latest state bằng `FINAL` đúng.

## 6. Production monitoring mở rộng

| Thành phần | Công cụ production |
|---|---|
| Flink | Prometheus metrics + Grafana |
| PostgreSQL | postgres_exporter |
| ClickHouse | clickhouse_exporter |
| Container | cAdvisor / Docker metrics |
| Log | Loki hoặc ELK |
| Alert | Alertmanager |

## 7. Alerting rules đề xuất

| Điều kiện | Cảnh báo |
|---|---|
| Flink job không RUNNING | CDC pipeline down |
| Replication slot inactive > 1 phút | Flink không đọc WAL |
| Retained WAL tăng nhanh | Nguy cơ đầy disk PostgreSQL |
| Checkpoint fail liên tục | Recovery không an toàn |
| ClickHouse row count không tăng | Sink có vấn đề |
| Latency > 10 giây | Không đạt near real-time |

## 8. Dashboard tối thiểu

| Nhóm | Ảnh minh chứng |
|---|---|
| Container | `docker ps` |
| PostgreSQL HA | `pg_stat_replication` |
| CDC slot | `pg_replication_slots` |
| Flink job | Flink UI |
| ClickHouse | Query `FINAL` |
| Fault tolerance | Restart TaskManager + query |

## 9. Checklist

- [ ] Có ảnh `docker ps`.
- [ ] Có ảnh Flink UI.
- [ ] Có query replication.
- [ ] Có query CDC slot.
- [ ] Có query ClickHouse.
- [ ] Có log/test restart.
- [ ] Có bảng monitoring production mở rộng.

## 10. Đoạn viết báo cáo

Trong phạm vi thực nghiệm, hệ thống được giám sát thông qua Flink Web UI, Docker logs, các bảng thống kê của PostgreSQL như `pg_stat_replication` và `pg_replication_slots`, cùng với truy vấn kiểm chứng trên ClickHouse. Các chỉ số quan trọng bao gồm trạng thái Flink job, slot active, replication streaming, số bản ghi đồng bộ và latency. Trong môi trường production, hệ thống có thể mở rộng monitoring bằng Prometheus/Grafana và các exporter cho Flink, PostgreSQL và ClickHouse.
