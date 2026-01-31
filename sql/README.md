
# Sales & Customer Analytics Platform – Database Design and Querying

## Overview


This part of the project focuses on designing a **normalized, efficient relational database schema** and implementing **analytical SQL queries** to support business insights.

The architecture follows a **modern data engineering pattern** with **Raw → Silver → Gold** layers:

![SQL DB Design](data_modelling.png)

- **Raw Layer:** Stores ingested sales and customer data exactly as it arrives (JSON → ```raw.sales_raw```, ```raw.customers_raw```).
- **Silver Layer:** Cleansed, validated, and transformed data suitable for analytical operations (```silver.sales```, ```silver.customers```).
- **Gold Layer:** Aggregated and enriched data optimized for reporting and business intelligence queries.

---

## Gold Layer Data Model

The Gold layer implements a **star-schema–like design** with one fact table and multiple dimension tables.


### 1. Customers Dimension

**Table:** `gold.customers`

| Column        | Description                    |
|---------------|--------------------------------|
| customer_id (PK) | Unique customer identifier    |
| customer_name | Full name                      |
| email         | Email address                  |
| region        | Customer region                |
| join_date     | Onboarding date                |
| loyalty_points| Accumulated loyalty points     |
| insert_date   | Record creation timestamp      |
| update_date   | Last update timestamp          |

---

### 2. Products Dimension

**Table:** `gold.products`

| Column        | Description                  |
|---------------|------------------------------|
| product_id (PK)| Unique product identifier    |
| product_name  | Product name                 |
| category      | Product category             |
| insert_date   | Record creation timestamp    |
| update_date   | Last update timestamp        |

---

### 3. Transactions Fact Table

**Table:** `gold.transactions`

| Column              | Description                                     |
|--------------------|-------------------------------------------------|
| transaction_id (PK) | Unique transaction identifier                  |
| customer_id (FK)    | References `gold.customers.customer_id`       |
| product_id (FK)     | References `gold.products.product_id`         |
| quantity            | Units sold                                     |
| discount            | Discount percentage                            |
| price               | Unit price                                     |
| region              | Transaction region                             |
| transaction_datetime| Transaction timestamp                           |
| total_value         | Computed transaction value                     |
| insert_date         | Record creation timestamp                      |
| update_date         | Last update timestamp                           |

**Computed Metric:**  
```sql
total_value = quantity * price * (1 - discount / 100)
```

### Silver → Gold Transformation Logic

- Data is promoted from Silver to Gold using **idempotent UPSERT logic**.
- Customers are merged from ``silver.customers`` into ``gold.customers``.
- Products are derived and deduplicated from ``silver.sales``.
- Transactions are enriched with computed ``total_value``.
- **ON CONFLICT** handling ensures no duplicate primary keys.

#### Benefits:

- Safe re-runs
- Slowly changing customer updates
- Clean analytical datasets

---

## Index Design and Performance Optimization

Indexes were created based on query access patterns to improve filtering, grouping, and join performance.

**Indexes Created**

```
-- Transactions by date (monthly trend)
CREATE INDEX idx_transactions_transaction_date 
ON gold.transactions (transaction_datetime);

-- Transactions by region
CREATE INDEX idx_transactions_region 
ON gold.transactions (region);

-- Foreign key: customer_id
CREATE INDEX idx_transactions_customer_id 
ON gold.transactions (customer_id);

-- Foreign key: product_id
CREATE INDEX idx_transactions_product_id 
ON gold.transactions (product_id);

-- Product category aggregation
CREATE INDEX idx_products_category 
ON gold.products (category);

-- Composite index for region + date analytics
CREATE INDEX idx_transactions_region_date 
ON gold.transactions (region, transaction_datetime);

```

**Performance Impact**

- Foreign key indexes accelerate joins between fact and dimension tables.
- Date and region indexes optimize time-series and regional aggregations.
- Precomputed total_value reduces repetitive calculations in queries.


------

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
- Fact–dimension separation supports scalable analytics.
- Using computed columns (like ``total_value``) can reduce repetitive calculations in analytical queries.

## Outcome

This database design provides:

- Clean analytical schema
- High query performance
- Clear separation of ingestion, transformation, and consumption layers
- A production-ready foundation for dashboards and reporting