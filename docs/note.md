**ReplacingMergeTree**
Là MergeTree + Khả năng thay thế các bản ghi cũ(có nhiều bản ghi cùng một key, muốn giữ lại bản mới nhất)

**PostgreSQL 3 chân**
Mục đích 1: Mô phỏng production 1 node chính để ghi, 1 hoặc nhiều node replica để đọc / backup / dự phòng load balancer hoặc proxy để điều hướng kết nối
Mục đích 2: Kiểm tra CDC khi source có replication

- Postgres Primary ghi dữ liệu
- Replica nhận dữ liệu từ Primary
- Flink CDC đọc WAL từ Primary
- ClickHouse nhận dữ liệu phục vụ OLAP

Mục đích 3: Phân biệt OLTP production và OLAP sink
OLTP PostgreSQL cluster
↓ CDC
Flink CDC
↓
ClickHouse OLAP

chú thích
replicator: user cho physical replication
orders_pub: publication cho Flink CDC logical replication
REPLICA IDENTITY FULL: giúp UPDATE/DELETE gửi đủ thông tin cho CDC
