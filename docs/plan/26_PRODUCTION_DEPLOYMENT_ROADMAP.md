# 26. Production Deployment Roadmap

## 1. Mục tiêu

Tài liệu này đưa ra lộ trình từ Docker Compose local tới production-like system. Không nhất thiết triển khai hết, nhưng cần trình bày rõ phần đã làm, phần còn thiếu và hướng phát triển.

## 2. Trạng thái hiện tại

| Thành phần | Trạng thái |
|---|---|
| PostgreSQL source | Đã có |
| PostgreSQL HA | Đã có primary + 2 replica |
| Flink JobManager/TaskManager | Đã có |
| Flink connector | Đã copy jar vào image riêng |
| ClickHouse sink | Đã có |
| HAProxy | Đã có |
| Docker volumes/network | Đã có |
| Functional test | Cần chốt ảnh/log |
| Benchmark | Cần đo |
| Monitoring production | Chưa có |
| Security hardening | Chưa có |
| Kubernetes/CI-CD | Future work |

## 3. Roadmap theo phase

### Phase 1 — Core pipeline

Mục tiêu: chạy end-to-end.

- Start Docker Compose.
- Kiểm tra primary/replica.
- Chạy Flink CDC job.
- Kiểm tra ClickHouse nhận dữ liệu.
- Test INSERT/UPDATE/SOFT DELETE.

Đầu ra: pipeline chạy được, có ảnh minh chứng.

### Phase 2 — Benchmark và correctness

Mục tiêu: chứng minh pipeline đúng và đo được.

- Script sinh dữ liệu.
- Đo latency.
- Đo throughput.
- Count/sum/status check.
- Restart TaskManager.

Đầu ra: bảng kết quả thực nghiệm.

### Phase 3 — Production-like hardening

Mục tiêu: bổ sung cơ chế gần production.

- Soft delete chuẩn.
- `ReplacingMergeTree` chuẩn.
- Checkpoint config rõ.
- Runbook vận hành.
- Monitoring checklist.
- `.env` cho secrets.

### Phase 4 — Advanced streaming

Mục tiêu: thêm event-time analytics.

- Watermark.
- Window aggregation.
- Realtime metrics table.
- Dashboard query.

### Phase 5 — Scale-out/Future work

Mục tiêu: định hướng production thật.

- Kafka.
- Prometheus/Grafana.
- StarRocks comparison.
- Schema evolution.
- Kubernetes.
- CI/CD.

## 4. Mức ưu tiên

| Ưu tiên | Nội dung |
|---|---|
| P0 | Core CDC, HA, INSERT/UPDATE/DELETE |
| P1 | Latency, throughput, correctness, fault tolerance |
| P1 | Idempotent, soft delete, checkpoint |
| P2 | Event-time metrics, savepoint demo |
| P3 | Kafka, StarRocks, Grafana, Kubernetes |

## 5. Production readiness matrix

| Tiêu chí | Hiện tại | Production cần thêm |
|---|---|---|
| Deployment | Docker Compose | Kubernetes/managed service |
| Source DB | PostgreSQL HA mô phỏng | HA thật, backup, failover |
| CDC | Flink CDC | Alerting, checkpoint tuning |
| Sink | ClickHouse local | Cluster/replication/backup |
| Monitoring | Manual + UI | Prometheus/Grafana |
| Security | Password đơn giản | Secret manager, RBAC, TLS |
| Data quality | Manual query | Automated validation |
| Scaling | 1 JM/TM | HA Flink cluster |
| Schema change | Manual | Migration workflow |

## 6. Rủi ro và giảm thiểu

| Rủi ro | Ảnh hưởng | Giảm thiểu |
|---|---|---|
| Flink job failed | Dừng đồng bộ | Checkpoint + alert |
| ClickHouse down | Không ghi sink | Retry + Kafka future |
| Slot giữ WAL | Đầy disk PostgreSQL | Monitor retained WAL |
| Duplicate event | Sai số liệu | Idempotent sink |
| Out-of-order | Trạng thái sai | Version/event_time |
| Schema change | Job fail | Schema evolution process |
| Password lộ | Rủi ro bảo mật | Secrets management |

## 7. Đoạn viết báo cáo

Dự án được triển khai theo lộ trình nhiều giai đoạn. Giai đoạn đầu tập trung xây dựng pipeline CDC end-to-end từ PostgreSQL sang ClickHouse bằng Flink CDC và kiểm thử PostgreSQL primary-replica. Giai đoạn tiếp theo bổ sung đo kiểm latency, throughput, correctness và fault tolerance. Sau đó hệ thống có thể được hardening theo hướng production-like thông qua checkpoint/savepoint, idempotent sink, monitoring, data quality validation và security. Các hướng mở rộng dài hạn bao gồm Kafka, event-time analytics, StarRocks, schema evolution và Kubernetes.
