-- CREATE DATABASE IF NOT EXISTS ecommerce_ods;

-- CREATE TABLE IF NOT EXISTS ecommerce_ods.orders_sink
-- (
--     order_id Int64,
--     customer_id Int64,
--     status String,
--     amount Decimal(12, 2),
--     created_at DateTime,
--     updated_at DateTime,
--     deleted UInt8 DEFAULT 0
-- )
-- ENGINE = ReplacingMergeTree(updated_at)
-- ORDER BY order_id;

-- DROP TABLE IF EXISTS ecommerce_ods.orders_sink;

-- CREATE TABLE ecommerce_ods.orders_sink
-- (
--     order_id Int64,
--     customer_id Int64,
--     status String,
--     amount Decimal(12, 2),
--     created_at DateTime64(3),
--     updated_at DateTime64(3),
--     deleted UInt8 DEFAULT 0
-- )
-- ENGINE = ReplacingMergeTree(updated_at)
-- ORDER BY order_id;


CREATE DATABASE IF NOT EXISTS ecommerce_ods;

DROP TABLE IF EXISTS ecommerce_ods.customers_sink;
DROP TABLE IF EXISTS ecommerce_ods.products_sink;
DROP TABLE IF EXISTS ecommerce_ods.orders_sink;
DROP TABLE IF EXISTS ecommerce_ods.order_items_sink;
DROP TABLE IF EXISTS ecommerce_ods.payments_sink;

CREATE TABLE ecommerce_ods.customers_sink
(
    customer_id Int64,
    full_name String,
    email String,
    phone Nullable(String),
    gender Nullable(String),
    date_of_birth Nullable(Date),
    address Nullable(String),
    city Nullable(String),
    country Nullable(String),
    customer_status String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id;

CREATE TABLE ecommerce_ods.products_sink
(
    product_id Int64,
    product_name String,
    category String,
    brand Nullable(String),
    price Decimal(18, 2),
    cost Nullable(Decimal(18, 2)),
    stock_quantity Int32,
    product_status String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

CREATE TABLE ecommerce_ods.orders_sink
(
    order_id Int64,
    customer_id Int64,
    order_status String,
    order_date DateTime64(3),
    total_amount Decimal(18, 2),
    discount_amount Decimal(18, 2),
    shipping_fee Decimal(18, 2),
    final_amount Decimal(18, 2),
    payment_status String,
    shipping_address Nullable(String),
    shipping_city Nullable(String),
    shipping_country Nullable(String),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;

CREATE TABLE ecommerce_ods.order_items_sink
(
    order_item_id Int64,
    order_id Int64,
    product_id Int64,
    quantity Int32,
    unit_price Decimal(18, 2),
    discount_amount Decimal(18, 2),
    total_amount Decimal(18, 2),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_item_id;

CREATE TABLE ecommerce_ods.payments_sink
(
    payment_id Int64,
    order_id Int64,
    payment_method String,
    payment_status String,
    payment_amount Decimal(18, 2),
    transaction_code Nullable(String),
    paid_at Nullable(DateTime64(3)),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY payment_id;