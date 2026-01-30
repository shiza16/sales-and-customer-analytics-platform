# Sales ETL Pipeline (Python + PostgreSQL)

## 📌 Overview

This project implements an **end-to-end ETL pipeline** for ingesting, validating, transforming, and loading sales data using **Python and PostgreSQL**.

The pipeline follows a **Raw → Silver** data architecture and mirrors real-world data engineering patterns used in modern analytics platforms such as **Microsoft Fabric, Databricks, and Airflow-based batch pipelines**.

Key features include:
- File-based ingestion
- Incremental processing using watermarks
- Data quality validation
- Error handling & structured logging
- Idempotent loads using UPSERT logic

---

## 🏗️ Architecture

### 📁 Project Structure

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
git status
