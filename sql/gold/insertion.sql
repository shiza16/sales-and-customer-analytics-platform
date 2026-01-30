
INSERT INTO gold.customers (
    customer_id,
    customer_name,
    email,
    region,
    join_date,
    loyalty_points,
    insert_date,
    update_date
)
SELECT
    customer_id,
    customer_name,
    email,
    region,
    join_date,
    loyalty_points,
    insert_date,
    NOW()
FROM silver.customers
ON CONFLICT (customer_id)
DO UPDATE SET
    customer_name   = EXCLUDED.customer_name,
    email           = EXCLUDED.email,
    region          = EXCLUDED.region,
    join_date       = EXCLUDED.join_date,
    loyalty_points = EXCLUDED.loyalty_points,
    update_date    = NOW();
   
INSERT INTO gold.products (
    product_id,
    product_name,
    category,
    insert_date,
    update_date
)
SELECT
    DISTINCT product_id,
    product_name,
    category,
    NOW(),
    NOW()
FROM silver.sales ;


INSERT INTO gold.transactions (
    transaction_id,
    customer_id,
    product_id,
    quantity,
    discount,
    price,
    total_value,
    region,
    transaction_datetime,
    insert_date,
    update_date
)
SELECT
    s.transaction_id,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.discount,
    s.price,
    (s.quantity * s.price * (1 - s.discount / 100)) AS total_value,
    s.region,
    s.date,
    s.insert_date,
    NOW()
FROM silver.sales s
ON CONFLICT (transaction_id)
DO UPDATE SET
    quantity         = EXCLUDED.quantity,
    discount         = EXCLUDED.discount,
    price            = EXCLUDED.price,
    total_value      = EXCLUDED.total_value,
    region           = EXCLUDED.region,
    transaction_date = EXCLUDED.transaction_date,
    update_date      = NOW();

-----------------------------------------------------------------------------

select * from gold.customers;
select * from gold.products;
select * from gold.transactions;