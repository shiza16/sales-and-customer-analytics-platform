CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.sales;
CREATE TABLE IF NOT EXISTS silver.sales (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    price NUMERIC(10,2),
    quantity INT,
    discount NUMERIC(5,2),
    date TIMESTAMP,
    region TEXT,
    insert_date TIMESTAMP,
    update_date TIMESTAMP
);


DROP TABLE IF EXISTS silver.customers;
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id      VARCHAR(20) PRIMARY KEY,
    customer_name    VARCHAR(100) NOT NULL,
    email            VARCHAR(150),
    region           VARCHAR(20) NOT NULL DEFAULT 'NA',
    join_date        DATE,
    loyalty_points   INTEGER NOT NULL DEFAULT 0,
    insert_date      TIMESTAMP NOT NULL,
    update_date      TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.etl_metadata;
CREATE TABLE IF NOT EXISTS silver.etl_metadata (
    pipeline_name TEXT PRIMARY KEY,
    last_insert_date TIMESTAMP
);
