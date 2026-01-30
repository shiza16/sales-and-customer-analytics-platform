CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.transactions;
DROP TABLE IF EXISTS gold.products;
DROP TABLE IF EXISTS gold.customers;


CREATE TABLE gold.customers (
    customer_id      VARCHAR(20) PRIMARY KEY,
    customer_name    VARCHAR(255) NOT NULL,
    email            VARCHAR(255),
    region           VARCHAR(50),
    join_date        DATE,
    loyalty_points   INT DEFAULT 0,
    insert_date      TIMESTAMP NOT NULL,
    update_date      TIMESTAMP NOT NULL
);


CREATE TABLE gold.products (
    product_id     VARCHAR(20) PRIMARY KEY,
    product_name   VARCHAR(255) NOT NULL,
    category       VARCHAR(100),
    insert_date    TIMESTAMP NOT NULL,
    update_date    TIMESTAMP NOT NULL
);


CREATE TABLE gold.transactions (
    transaction_id  VARCHAR(30) PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,
    product_id      VARCHAR(20) NOT NULL,
    quantity        INT NOT NULL CHECK (quantity > 0),
    discount NUMERIC(5,2) DEFAULT 0 CHECK (discount BETWEEN 0 AND 100),
    price          NUMERIC(12,2) NOT NULL,
    total_value       NUMERIC(14,2),
    region          VARCHAR(50),
    transaction_datetime TIMESTAMP NOT NULL,
    insert_date     TIMESTAMP NOT NULL,
    update_date     TIMESTAMP NOT NULL,

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id)
        REFERENCES gold.customers(customer_id),

    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES gold.products(product_id)
);

--------------------------------------------------
/**
Index strategy, We index:

Join columns
Filter/group-by columns
Time-series column
**/

-- Index 1 — Transactions by date (monthly trend)
CREATE INDEX idx_transactions_transaction_date
ON gold.transactions (transaction_datetime);


-- Index 2 — Transactions by region
CREATE INDEX idx_transactions_region
ON gold.transactions (region);

-- Index 3 — Foreign key: customer_id
CREATE INDEX idx_transactions_customer_id
ON gold.transactions (customer_id);

-- Index 4 — Foreign key: product_id
CREATE INDEX idx_transactions_product_id
ON gold.transactions (product_id);

-- Index 5 — Product category (aggregation)
CREATE INDEX idx_products_category
ON gold.products (category);

--Composite index for heavy analytics
CREATE INDEX idx_transactions_region_date
ON gold.transactions (region, transaction_datetime);