CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE orders REPLICA IDENTITY FULL;
-- cái này cho phép nhận diện một dòng khi gửi ra ngoài logical repplication,
-- full có nghĩa là lấy tất cả dữ liệu của bảng đó đề gửi vào wal

CREATE PUBLICATION orders_pub FOR TABLE orders;
-- chỉ định và tao nên nói cho phép nói là bảng order được phép phát các thay đổi ra bên ngoài WAL 

INSERT INTO orders(order_id, customer_id, status, amount)
VALUES
(1, 101, 'CREATED', 120000),
(2, 102, 'CREATED', 250000),
(3, 103, 'PAID', 310000);