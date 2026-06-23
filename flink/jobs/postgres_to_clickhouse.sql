SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'ecommerce-postgres-to-clickhouse-cdc';

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';

CREATE TABLE postgres_customers (
    customer_id BIGINT,
    full_name STRING,
    email STRING,
    phone STRING,
    gender STRING,
    date_of_birth DATE,
    address STRING,
    city STRING,
    country STRING,
    customer_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'customers',
    'slot.name' = 'flink_customers_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_categories (
    category_id BIGINT,
    category_name STRING,
    description STRING,
    category_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'categories',
    'slot.name' = 'flink_categories_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_products (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    brand STRING,
    price DECIMAL(18, 2),
    cost DECIMAL(18, 2),
    stock_quantity INT,
    product_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_products_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_date TIMESTAMP(3),
    total_amount DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    shipping_fee DECIMAL(18, 2),
    final_amount DECIMAL(18, 2),
    payment_status STRING,
    shipping_address STRING,
    shipping_city STRING,
    shipping_country STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_orders_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_order_items (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    total_amount DECIMAL(18, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'order_items',
    'slot.name' = 'flink_order_items_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_payments (
    payment_id BIGINT,
    order_id BIGINT,
    payment_method STRING,
    payment_status STRING,
    payment_amount DECIMAL(18, 2),
    transaction_code STRING,
    paid_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'payments',
    'slot.name' = 'flink_payments_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_shipments (
    shipment_id BIGINT,
    order_id BIGINT,
    carrier STRING,
    tracking_number STRING,
    shipment_status STRING,
    shipped_at TIMESTAMP(3),
    delivered_at TIMESTAMP(3),
    shipping_address STRING,
    shipping_city STRING,
    shipping_country STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (shipment_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'shipments',
    'slot.name' = 'flink_shipments_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE postgres_inventory_movements (
    movement_id BIGINT,
    product_id BIGINT,
    order_id BIGINT,
    movement_type STRING,
    quantity_change INT,
    old_stock INT,
    new_stock INT,
    reason STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (movement_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'ecommerce_ods',
    'schema-name' = 'public',
    'table-name' = 'inventory_movements',
    'slot.name' = 'flink_inventory_movements_slot',
    'decoding.plugin.name' = 'pgoutput'
);


CREATE TABLE clickhouse_customers (
    customer_id BIGINT,
    full_name STRING,
    email STRING,
    phone STRING,
    gender STRING,
    date_of_birth DATE,
    address STRING,
    city STRING,
    country STRING,
    customer_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'customers_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_categories (
    category_id BIGINT,
    category_name STRING,
    description STRING,
    category_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'categories_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_products (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    brand STRING,
    price DECIMAL(18, 2),
    cost DECIMAL(18, 2),
    stock_quantity INT,
    product_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'products_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_date TIMESTAMP(3),
    total_amount DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    shipping_fee DECIMAL(18, 2),
    final_amount DECIMAL(18, 2),
    payment_status STRING,
    shipping_address STRING,
    shipping_city STRING,
    shipping_country STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'orders_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_order_items (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    total_amount DECIMAL(18, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'order_items_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_payments (
    payment_id BIGINT,
    order_id BIGINT,
    payment_method STRING,
    payment_status STRING,
    payment_amount DECIMAL(18, 2),
    transaction_code STRING,
    paid_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'payments_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_shipments (
    shipment_id BIGINT,
    order_id BIGINT,
    carrier STRING,
    tracking_number STRING,
    shipment_status STRING,
    shipped_at TIMESTAMP(3),
    delivered_at TIMESTAMP(3),
    shipping_address STRING,
    shipping_city STRING,
    shipping_country STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (shipment_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'shipments_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);

CREATE TABLE clickhouse_inventory_movements (
    movement_id BIGINT,
    product_id BIGINT,
    order_id BIGINT,
    movement_type STRING,
    quantity_change INT,
    old_stock INT,
    new_stock INT,
    reason STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted_at TIMESTAMP(3),
    PRIMARY KEY (movement_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'ecommerce_ods',
    'table-name' = 'inventory_movements_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'
);


EXECUTE STATEMENT SET
BEGIN

INSERT INTO clickhouse_customers
SELECT * FROM postgres_customers;

INSERT INTO clickhouse_categories
SELECT * FROM postgres_categories;

INSERT INTO clickhouse_products
SELECT * FROM postgres_products;

INSERT INTO clickhouse_orders
SELECT * FROM postgres_orders;

INSERT INTO clickhouse_order_items
SELECT * FROM postgres_order_items;

INSERT INTO clickhouse_payments
SELECT * FROM postgres_payments;

INSERT INTO clickhouse_shipments
SELECT * FROM postgres_shipments;

INSERT INTO clickhouse_inventory_movements
SELECT * FROM postgres_inventory_movements;

END;
