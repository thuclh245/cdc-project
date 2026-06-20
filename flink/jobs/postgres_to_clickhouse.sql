SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'postgres-to-clickhouse-cdc';

CREATE TABLE postgres_orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    deleted BOOLEAN,
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
    deleted INT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://clickhouse:8123',
    'database-name' = 'cdc_demo',
    'table-name' = 'orders_sink',
    'username' = 'default',
    'password' = '',
    'sink.update-strategy' = 'insert'  
);


INSERT INTO clickhouse_orders
SELECT
    order_id,
    customer_id,
    status,
    amount,
    created_at,
    updated_at,
    CAST(deleted AS INT) AS deleted
FROM postgres_orders;