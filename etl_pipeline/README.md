# Sales ETL Pipeline (Python + PostgreSQL)

## Overview

This project implements an **end-to-end ETL pipeline** for ingesting, validating, transforming, and loading sales data using **Python and PostgreSQL**.

The pipeline follows a **Raw → Silver** data architecture and mirrors real-world data engineering patterns used in modern analytics platforms such as **Microsoft Fabric, Databricks, and Airflow-based batch pipelines**.

The pipeline is designed to:

- Scale from small sample data to **hundreds of records**
- Handle **real-world data quality issues**
- Support **incremental and idempotent processing**
- Ensure **repeatable and fault-tolerant execution**

The implementation follows modern data engineering practices commonly used in platforms such as **Microsoft Fabric, Databricks, and Airflow-based batch pipelines**.


---

## Dataset Description

**Source Dataset:** `sales_data.json`

The dataset contains transactional sales records with nested product attributes and intentionally inconsistent formats to simulate real-world ingestion challenges.

### Key Data Challenges

- Nested JSON structures (`product` object)
- Multiple date formats
- Missing and null fields
- Invalid business values (e.g., negative quantities)
- Mixed discount representations

To simulate production-scale ingestion, the base dataset was **programmatically expanded to 500–1,000 records**.

---

## Sales Data Generation Strategy

A custom Python-based data generation script `sales_data_generation.ipynb` was created to test pipeline robustness and validation logic.

### Key Characteristics

- Duplicates and modifies base records to increase volume
- Preserves referential integrity for valid customer IDs
- Injects controlled data quality issues:
  - Null `customer_id`
  - Negative `quantity`
  - Missing or invalid `discount`
- Randomizes:
  - Transaction timestamps (within 2023)
  - Regions
  - Product prices (minor variations)

### Purpose

This approach allows the pipeline to be evaluated against:

- Realistic data errors
- Data quality validation logic
- Error handling and observability
- Restartability
- Downstream analytics readiness

The generated output is written to the **Raw layer** as JSON files.

---

## ETL Architecture Overview

The pipeline follows a **Raw → Silver layered architecture**, a common pattern in enterprise analytics platforms.

![Python ETL Architecture](etl_pipeline/src/utils/python_etl_architecture.png)



## 🗄️ Database Layers

| Layer  | Table | Description |
|------|------|-------------|
| Raw | raw.sales_raw | Append-only raw ingested data |
| Silver | silver.sales | Cleaned, deduplicated transactional data |
| Silver | silver.etl_metadata | Stores incremental load watermark |



## ETL Flow

**1. Extract (File → Raw Table)**

**Source:**
- JSON files located in ``data/raw/``

**Steps:**
- Check if file exists (non-fatal if missing)
- Read JSON records using Pandas
- Insert records into ```raw.sales_raw```
- Stamp each record with ``insert_date``
- Move successfully processed files to ```data/processed/```

**Failure Handling:**
- Missing or empty files are logged and skipped
- Errors are logged without crashing the pipeline

**2. Incremental Extract (Raw → DataFrame)**

**Logic:**
- Read ```last_insert_date``` from ```silver.etl_metadata```
- Extract records from ```raw.sales_raw``` 

``` #sql
where insert_date > last_insert_date
```

**First Run Behavior:**
- If no metadata exists, defaults to ```1900-01-01```
- Treats it as a full initial load

**3. Transform & Data Quality Checks**

Transformations and validations are performed using **vectorized Pandas logic** for performance.

**Key Transformations:**
- Normalize ```customer_id```
- Fill missing discount values with ``0``
- Parse dates safely
- Add ```update_date```

**Data Quality Rules:**

Records are marked invalid if:

- ```transaction_id``` is NULL
- ```customer_id``` is NULL
- ```price``` is NULL
- ```quantity``` <= 0
- ```discount``` is outside ```[0, 1]```


Each invalid record receives a human-readable dq_errors field, e.g.:

```
"Missing customer_id; Invalid quantity"
```

Valid and invalid records are separated cleanly.

**4. Load (Silver Layer)**

**Target Table:**

- ```silver.sales```

**Strategy:**
- UPSERT using ``transaction_id`` as the primary key

```
ON CONFLICT (transaction_id) 
DO UPDATE SET ...
```

**Ensures:**
- Idempotent loads
- Updates existing transactions
- No duplicate records

**5. Metadata Update**

After a successful Silver load:

- The maximum ```insert_date``` from ```raw.sales_raw``` is written to ```silver.etl_metadata```
- This value becomes the watermark for the next run

**6. Invalid Data Logging**

- Invalid records are converted to JSON-safe format
- Written to ```logs/invalid_sales.json```
- Fully auditable with detailed error reasons


---
---

## Incremental Logic Summary


| Component           | Purpose    | 
|---------------------|-------------|
| ``insert_date``        | Raw ingestion timestamp |
| ``silver.etl_metadata`` | Stores processing watermark | 
| Watermark filter    | Prevents reprocessing|
| UPSERT              |	Guarantees idempotency |


The watermark-based design ensures that **only new or changed records** are processed on each run.
Combined with UPSERT logic, multiple executions always result in the **same final state**.

This supports:
- Safe re-runs
- Partial failures without corruption
- Scalable batch processing


## Logging & Observability

Structured logging captures:
- Pipeline start and end events
- Record counts per stage
- Data quality violations
- Database connectivity issues
- Errors with stack traces

Log file: 

```
logs/sales_etl_logs.log
```

---
---

## Technology Stack

- Python 3
- PostgreSQL
- Pandas
- psycopg2
- JSON-based ingestion
- File-based data lake simulation

## Why This Design? 

- Raw/Silver separation enables replay, auditing, and fault tolerance 
- Watermark-based incremental loading avoids reprocessing 
- UPSERT logic ensures idempotency and safe re-runs 
- Vectorized Pandas transformations improve performance at scale 

## Outcome

This ETL pipeline demonstrates:

- Incremental data processing
- Strong data validation and auditing
- Fault tolerance and restartability
- Analytics-ready data modeling

It forms a solid foundation for downstream analytical workloads and BI consumption, closely resembling real-world production data engineering pipelines.