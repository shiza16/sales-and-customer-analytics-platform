-- Total sales by region and category

SELECT
    t.region, p.category, SUM(t.total_value) AS total_sales
FROM gold.transactions t JOIN gold.products p ON t.product_id = p.product_id
GROUP BY  t.region, p.category
ORDER BY  t.region, total_sales DESC;
    
   
-- Top 5 products by total revenue
SELECT
    p.product_id, p.product_name, SUM(t.total_value) AS total_revenue
FROM gold.transactions t JOIN gold.products p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 5;   

-- Monthly sales trend
SELECT
	TO_CHAR(t.transaction_date, 'YYYY-MM') AS sales_month,
    SUM(t.total_value) AS monthly_sales
FROM gold.transactions t
GROUP BY
    TO_CHAR(t.transaction_date, 'YYYY-MM') 
ORDER BY  sales_month;
    
-- Average discount percentage per region
SELECT
    region, 
    AVG(discount)  AS avg_discount_percentage
FROM gold.transactions
GROUP BY region
ORDER BY avg_discount_percentage DESC;
    
-- Number of transactions with total_value > $1000
SELECT
    COUNT(*) AS high_value_transactions
FROM gold.transactions
WHERE total_value > 1000;