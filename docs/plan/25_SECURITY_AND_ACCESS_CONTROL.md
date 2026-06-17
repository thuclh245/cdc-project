# 25. Security và Access Control

## 1. Mục tiêu

V1 chạy local nên có thể dùng cấu hình đơn giản. Nhưng để production-like cần phân tích bảo mật:

- User/password.
- Quyền truy cập.
- Secrets.
- Network exposure.
- TLS.
- Log dữ liệu nhạy cảm.

## 2. Vấn đề trong lab

Cấu hình lab thường dùng:

```yaml
POSTGRES_USER: postgres
POSTGRES_PASSWORD: postgres
```

Điều này chấp nhận được để demo nhưng không production vì:

- Dùng superuser.
- Mật khẩu hard-code.
- Port DB mở ra host.
- Chưa tách user CDC/app/read-only.
- Chưa có TLS/secrets manager.

## 3. Nguyên tắc production

| Nguyên tắc | Ý nghĩa |
|---|---|
| Least privilege | User chỉ có quyền cần thiết |
| No hard-coded secrets | Không ghi mật khẩu trong compose |
| Network isolation | Chỉ service cần thiết được truy cập |
| Role separation | Tách admin/app/cdc/readonly |
| Auditability | Có log truy cập và thay đổi |
| Secure transport | TLS nếu qua network không tin cậy |

## 4. PostgreSQL users đề xuất

CDC user:

```sql
CREATE USER flink_cdc WITH REPLICATION LOGIN PASSWORD 'change_me';
GRANT CONNECT ON DATABASE cdc_demo TO flink_cdc;
GRANT USAGE ON SCHEMA public TO flink_cdc;
GRANT SELECT ON orders TO flink_cdc;
```

Application user:

```sql
CREATE USER app_user WITH PASSWORD 'change_me';
GRANT CONNECT ON DATABASE cdc_demo TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO app_user;
```

Readonly user:

```sql
CREATE USER readonly_user WITH PASSWORD 'change_me';
GRANT CONNECT ON DATABASE cdc_demo TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
```

## 5. ClickHouse users đề xuất

| User | Quyền |
|---|---|
| `clickhouse_ingest_user` | INSERT vào bảng sink |
| `clickhouse_readonly_user` | SELECT dashboard/report |
| `clickhouse_admin_user` | Quản trị schema |

## 6. Secrets

Dùng `.env`:

```env
POSTGRES_PASSWORD=change_me
REPL_PASSWORD=change_me
CLICKHOUSE_PASSWORD=change_me
```

Compose:

```yaml
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

`.gitignore`:

```text
.env
*.key
*.pem
secrets/
```

## 7. Network security

Trong lab đang expose nhiều port:

```text
5432, 5433, 5434, 5435, 8123, 9000, 8081, 15432, 15433
```

Production-like nên:

- Chỉ expose HAProxy hoặc API cần thiết.
- Không expose replica ra public.
- Giới hạn Flink UI.
- Giữ giao tiếp nội bộ qua Docker network/Kubernetes service.

## 8. Logging

Không log:

- Password.
- Connection string đầy đủ.
- Token/API key.
- Dữ liệu cá nhân không cần thiết.

Nếu dùng dữ liệu fake thì ghi rõ trong báo cáo để tránh vấn đề riêng tư.

## 9. Checklist

- [ ] Không dùng superuser cho CDC trong production.
- [ ] Có user riêng cho CDC.
- [ ] Có user riêng cho ingest/read-only.
- [ ] Không hard-code password.
- [ ] Có `.env` và `.gitignore`.
- [ ] Không expose port không cần thiết.
- [ ] Có mô tả security limitation.
- [ ] Có hướng mở rộng TLS/secret manager.

## 10. Đoạn viết báo cáo

Trong phạm vi thực nghiệm, hệ thống sử dụng cấu hình đơn giản để thuận tiện triển khai bằng Docker Compose. Tuy nhiên, khi hướng tới production cần tách riêng các tài khoản theo vai trò như application user, CDC user và readonly user, đồng thời áp dụng nguyên tắc least privilege. Mật khẩu không nên hard-code trong file cấu hình mà cần được quản lý qua biến môi trường hoặc secret manager. Các port không cần thiết không nên expose ra bên ngoài, và các kết nối quan trọng có thể bổ sung TLS khi triển khai trên môi trường mạng không tin cậy.
