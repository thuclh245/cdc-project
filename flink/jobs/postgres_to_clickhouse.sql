SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'postgres-to-clickhouse-cdc';

CREATE TABLE postgres_orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'pg-primary',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'cdc_demo',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_orders_slot_ha',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE clickhouse_orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'cdc_demo',
    'table-name' = 'orders_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'  -- Chuyển các bản ghi UPDATE_AFTER thành INSERT
);


INSERT INTO clickhouse_orders
SELECT
    order_id,
    customer_id,
    status,
    amount,
    created_at,
    updated_at
FROM postgres_orders;