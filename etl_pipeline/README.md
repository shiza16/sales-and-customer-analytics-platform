# Sales ETL Pipeline (Python + PostgreSQL)

## Overview

This project implements an **end-to-end ETL pipeline** for ingesting, validating, transforming, and loading sales data using **Python and PostgreSQL**.

The pipeline follows a **Raw → Silver** data architecture and mirrors real-world data engineering patterns used in modern analytics platforms such as **Microsoft Fabric, Databricks, and Airflow-based batch pipelines**.

Key features include:
- File-based ingestion
- Incremental processing using watermarks
- Data quality validation
- Error handling & structured logging
- Idempotent loads using UPSERT logic


---
---

## Architecture

Project structure:

```text
data/
├── raw/                     # Incoming JSON files (landing zone)
├── processed/               # Successfully ingested files

etl_pipeline/
├── src/
│   ├── sales_etl_functions.py
│   └── utils/
│       ├── config.py
│       └── logger.py

logs/
├── sales_etl_logs.log       # Pipeline execution logs
├── invalid_sales.json       # Invalid records with DQ errors

```

---
---

## 🗄️ Database Layers

| Layer  | Table | Description |
|------|------|-------------|
| Raw | raw.sales_raw | Append-only raw ingested data |
| Silver | silver.sales | Cleaned, deduplicated transactional data |
| Silver | silver.etl_metadata | Stores incremental load watermark |

---
---

## ETL Flow

**1. Extract (File → Raw Table)**

**Source:**
- JSON files located in data/raw/

**Steps:**
- Check if file exists (non-fatal if missing)
- Read JSON records
- Insert records into ```raw.sales_raw```
- Stamp each record with insert_date
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
| insert_date         | raw.sales_raw |
| silver.etl_metadata | silver.sales | 
| Watermark filter    | Prevents reprocessing old data|
| UPSERT              |	Guarantees idempotency |


This design allows:
- Safe re-runs
- Partial failures without corruption
- Scalable batch processing

---
---

## Logging & Observability

Logs capture:
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

## Tech Stack

- Python 3
- PostgreSQL
- Pandas
- psycopg2
- JSON-based ingestion
- File-based data lake simulation
