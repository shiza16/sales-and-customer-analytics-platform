import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Part1 folder

DATA_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../..", "data/raw", "sales_data_scaled.json"))
LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../..","logs", "sales_etl_logs.log"))
INVALID_SALES_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../..", "logs", "invalid_sales.json"))

PROCESSED_DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../..", "data/processed"))

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "sales_db",
    "user": "postgres",
    "password": "12345678"
}

