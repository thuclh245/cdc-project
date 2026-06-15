CREATE DATABASE IF NOT EXISTS cdc_demo;

CREATE TABLE IF NOT EXISTS cdc_demo.orders_sink
(
    order_id Int64,
    customer_id Int64,
    status String,
    amount Decimal(12, 2),
    created_at DateTime,
    updated_at DateTime,
    deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;