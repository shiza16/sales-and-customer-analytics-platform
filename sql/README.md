
# Sales & Customer Analytics Platform – Part 2: Database Design and Querying

## Overview


This part of the project focuses on designing a **normalized, efficient relational database schema** and implementing **analytical SQL queries** to support business insights.

The architecture follows a **modern data engineering pattern** with **Raw → Silver → Gold** layers:

- **Raw Layer:** Stores ingested sales and customer data exactly as it arrives (JSON → ```raw.sales_raw```, ```raw.customers_raw```).
- **Silver Layer:** Cleansed, validated, and transformed data suitable for analytical operations (```silver.sales```, ```silver.customers```).
- **Gold Layer:** Aggregated and enriched data optimized for reporting and business intelligence queries.

---

## Database Schema

**Tables**

**1. Customers**

- ```customer_id``` (PK)
- ```customer_name```
- ```email```
- ```region```
- ```join_date```
- ```loyalty_points```

**2. Products**

- ```product_id``` (PK)
- ```product_name```
- ```category```
- ```price```

**3. Transactions**

- ```transaction_id``` (PK)
- ```customer_id``` (FK → Customers.customer_id)
- ```product_id``` (FK → Products.product_id)
- ```quantity```
- ```discount```
- ```region```
- ```price```
- ```date```
- ```transaction_datetime```
- ```total_value``` (computed: price × quantity × (1 - discount))

--- 
## Index Design and Performance Optimization

Indexes were created based on query access patterns to improve filtering, grouping, and join performance.

**Indexes Created**

```
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

-- Composite index for heavy analytics
CREATE INDEX idx_transactions_region_date
ON gold.transactions (region, transaction_datetime);

```

**Performance Impact**

- Foreign key indexes accelerate joins between fact and dimension tables.
- Date and region indexes optimize time-series and regional aggregations.


--

## ETL Layers

Raw Layer      | Silver Layer      |Gold Layer
-----------    |  -----------    | ---------
raw.sales_raw  |  silver.sales       |  gold.aggregates |
JSON data      |  Cleaned & Validated |   Aggregated/Analytics-ready |


- Raw layer keeps **original ingested data** for auditing and replay.
- Silver layer applies **DQ checks, transformations, and metadata watermarking**.
- Gold layer stores **pre-computed aggregates** for reporting.


---
## Analytical SQL Queries

The following queries demonstrate common business insights:

**1️. Total sales by region and category**

```
SELECT c.region, p.category, SUM(t.quantity * p.price * (1 - t.discount)) AS total_sales
FROM Transactions t
JOIN Customers c ON t.customer_id = c.customer_id
JOIN Products p ON t.product_id = p.product_id
GROUP BY c.region, p.category
ORDER BY c.region, p.category;
```

**2️. Top 5 products by total revenue**

```
SELECT p.product_name, SUM(t.quantity * p.price * (1 - t.discount)) AS total_revenue
FROM Transactions t
JOIN Products p ON t.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;
```

**3️. Monthly sales trend**

```
SELECT DATE_TRUNC('month', t.date) AS month,
       SUM(t.quantity * p.price * (1 - t.discount)) AS total_sales
FROM Transactions t
JOIN Products p ON t.product_id = p.product_id
GROUP BY month
ORDER BY month;
```

**4️. Average discount percentage per region**

```
SELECT c.region, AVG(t.discount * 100) AS avg_discount_pct
FROM Transactions t
JOIN Customers c ON t.customer_id = c.customer_id
GROUP BY c.region
ORDER BY c.region;
```

**5️. Number of transactions with total_value > $1000**

```
SELECT COUNT(*) AS high_value_transactions
FROM (
    SELECT t.transaction_id, t.quantity * p.price * (1 - t.discount) AS total_value
    FROM Transactions t
    JOIN Products p ON t.product_id = p.product_id
) sub
WHERE total_value > 1000;
```

---

## Performance Notes

- Indexes on ``date``, ``region``, and ``category`` significantly improve aggregation and filter queries.
- Normalization avoids data duplication while maintaining query efficiency.
- Using computed columns (like ``total_value``) can reduce repetitive calculations in analytical queries.