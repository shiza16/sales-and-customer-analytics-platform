CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.sales_raw;
CREATE TABLE IF NOT EXISTS raw.sales_raw (
    transaction_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    price NUMERIC(10,2),
    quantity INT,
    discount NUMERIC(5,2),
    date TIMESTAMP,
    region TEXT,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS raw.customers_raw;
CREATE TABLE raw.customers_raw (
    customer_id      TEXT,
    customer_name    TEXT,
    email            TEXT,
    region           TEXT,
    join_date        TEXT,         
    loyalty_points   TEXT,
    insert_date      TIMESTAMP
);
