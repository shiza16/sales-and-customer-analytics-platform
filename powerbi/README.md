# Revenue & Customer Insights – Power BI Dashboard


### Objective
The purpose of this dashboard is to visualize key insights from both the **Sales** and **Customer** datasets. It provides a comprehensive view of revenue trends, customer behavior, and product performance to support business decision-making.

### Data Source

This dashboard is built on **Gold Layer tables** (``gold.products``, ``gold.customers`` and ``gold.transactions``), which are aggregated, cleaned, and validated in the ETL pipeline. Using Gold Layer ensures that metrics like Total Sales, Average Sale per Transaction, and Loyalty Points are **accurate, consistent, and ready for reporting**.


### Dashboard Overview

**Dashboard Name:** Revenue & Customer Insights Dashboard

![Dashboard Screenshot](powerbi/powerbi_dashboard.png)

**Key Features:**
- Interactive filters for ``Region``, ``Product Name``, ``Product``, ``Category``, and ``Period``.
- Five core visuals designed for fast, executive-level insights:
  1. **Bar Chart:** Total Sales by Region.
  2. **Line Chart:** Monthly Sales Trend.
  3. **Table:** Top 5 Products by Revenue.
  4. **Cards:** Total Customers, Total Sales, Average Sale per Transaction, High-Value Transactions, Sales YTD.
  5. **Donut Chart:** Loyalty Points Distribution by Region.


All visuals support cross-filtering and drill-down, enabling users to explore insights interactively across regions, products, and time periods.

---

### DAX Measures

The following DAX measures were created to calculate key metrics:

1. **Total Sales**
```
Total Sales = SUM('gold transactions'[total_value])
```

2. **Average Sale per Transaction**
```
Avg Sale per Transaction = AVERAGE('gold transactions'[total_value])

```

3. **Total Loyalty Points**
```
Total Loyalty Points = SUM('gold customers'[loyalty_points])
```

4. **High-Value Transactions (>1000)**
```
High Value Transactions = CALCULATE( COUNT('gold transactions'[transaction_id]), 'gold transactions'[total_value] > 1000 )
```

5. **Sales YTD**

```
Sales YTD = TOTALYTD( [Total Sales], 'Date'[Date] )
```

6. **High Value Sales**
```
High Value Sales = 
CALCULATE (
    [Total Sales],
    'gold transactions'[total_value]> 1000
)

```

## Insights from the Dashboard


1. **Sales by Region**

- West region leads in total sales ($110K), followed by North, East, and South.

2. **Monthly Sales Trend**

- Highest sales occurred in **January (73K), September (64K)**, and **December (62K)**.

- Lowest sales were in **July (35K)**.

3. **Top Products**

- Laptops are the top revenue-generating product ($357K).

- Monitors and Desks follow, while Keyboard and Mouse contribute less.

4. **Customer Insights**

- Total of **99 customers**.
- Average sale per transaction is **$898**.
- High-value transactions (> $1000) total **169**.

5. **Loyalty Points Distribution**

- South region has the highest share of loyalty points (29.09%),  followed by North, West, and East.

## Assumptions & Notes

- Each transaction represents a single completed sale.
- Loyalty points are pre-calculated upstream in the Gold layer.
- Currency values are assumed to be standardized (USD).


## Design & Performance Notes

- Pre-aggregated Gold Layer tables significantly reduce Power BI computation overhead.
- Time-intelligence measures rely on a proper Date dimension to avoid ambiguity.
- KPI cards provide quick executive insights, while detailed visuals enable deeper analysis.
- The model is designed to scale with growing transaction volumes without requiring changes to the report layer.
