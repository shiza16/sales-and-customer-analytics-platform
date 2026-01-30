# Imports and configuration
import os
import json
import shutil
from datetime import datetime, date
import math
import psycopg2
from .utils.config import *
from .utils.logger import get_logger
import copy
import pandas as pd

logger = get_logger()


def check_db_connection(db_config):
    """
    Attempts to establish a connection to the database using the provided
    configuration. Logs and prints whether the connection was successful
    or failed. Returns True if connection succeeds, otherwise False.
    """
    try:
        conn = psycopg2.connect(**db_config)
        conn.close()
        logger.info("Database connection successful!")
        print("Database connection successful!")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        print(f"Database connection failed: {e}")
        return False


def load_sales_to_raw(json_file_path, processed_dir, db_config):
    """
    Loads a new sales JSON file from the raw folder into the raw.sales_raw table.
    If the file is successfully processed, it is moved to the processed folder.
    If the file is missing or empty, the pipeline continues without failing.
    """
    # Check if file exists
    if not os.path.exists(json_file_path):
        logger.warning(
            f"RAW file not found. Skipping load: {json_file_path}"
        )
        return 

    try:
        with open(json_file_path, "r") as f:
            sales_data = json.load(f)

        if not sales_data:
            logger.warning(
                f"RAW file is empty. Nothing to load: {json_file_path}"
            )
            return

        logger.info(
            f"Loaded {len(sales_data)} records from file {json_file_path}"
        )

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO raw.sales_raw (
                transaction_id,
                customer_id,
                product_id,
                product_name,
                category,
                price,
                quantity,
                discount,
                date,
                region,
                insert_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        insert_ts = datetime.utcnow()
        rows_inserted = 0

        for record in sales_data:
            cursor.execute(
                insert_sql,
                (
                    record.get("transaction_id"),
                    record.get("customer_id"),
                    record.get("product", {}).get("id"),
                    record.get("product", {}).get("name"),
                    record.get("product", {}).get("category"),
                    record.get("product", {}).get("price"),
                    record.get("quantity"),
                    record.get("discount"),
                    record.get("date"),
                    record.get("region"),
                    insert_ts
                )
            )
            rows_inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            f"Successfully inserted {rows_inserted} records into raw.sales_raw"
        )

        # Move file to processed folder
        os.makedirs(processed_dir, exist_ok=True)

        file_name = os.path.basename(json_file_path)
        processed_path = os.path.join(processed_dir, file_name)

        shutil.move(json_file_path, processed_path)

        logger.info(
            f"Moved file to processed folder: {processed_path}"
        )

    except Exception as e:
        logger.error(
            f"RAW load failed for file {json_file_path}: {e}",
            exc_info=True
        )
        return


def extract_sales_from_raw(db_config):
    """
    Extracts new records from raw.sales_raw based on the last successful
    insert date stored in silver.etl_metadata. If no metadata exists,
    assumes this is the first run and extracts all records.
    Returns the extracted data as a pandas DataFrame.
    """
    conn = psycopg2.connect(**db_config)

    last_insert_query = """
        SELECT last_insert_date
        FROM silver.etl_metadata
        WHERE pipeline_name = 'sales_silver'
    """

    df_meta = pd.read_sql(last_insert_query, conn)

    if df_meta.empty:
        last_insert_date = datetime(1900, 1, 1)
        logger.info(
            "No metadata found for pipeline sales_silver. "
            "Assuming first run with last_insert_date = 1900-01-01"
        )
    else:
        last_insert_date = df_meta.iloc[0, 0]
        logger.info(f"Last processed insert_date: {last_insert_date}")

    extract_query = """
        SELECT *
        FROM raw.sales_raw
        WHERE insert_date > %s
    """

    df = pd.read_sql(extract_query, conn, params=(last_insert_date,))
    conn.close()

    logger.info(
        f"Extracted {len(df)} new RAW records since {last_insert_date}"
    )

    return df


def parse_date_safe(date_str):
    """
    Safely parses a date string into a datetime object by trying multiple
    supported formats. If the date is null or cannot be parsed, returns None.
    """
    if not date_str:
        return None

    formats = [
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except Exception:
            continue
    return None


def transform_sales(df_raw):
    """
    Transforms raw sales data by cleaning fields, parsing dates,
    applying business rules, and performing data quality checks.
    Splits the data into valid and invalid records based on DQ errors.
    Returns two DataFrames: (valid_records, invalid_records).
    """
    logger.info("Starting sales transformation")

    if df_raw.empty:
        logger.warning("Received empty DataFrame for transformation")
        return df_raw, pd.DataFrame()

    start_time = datetime.utcnow()

    df = df_raw.copy()
    total_records = len(df)

    logger.info(f"Input records count: {total_records}")

    df['customer_id'] = df['customer_id'].str.strip().str.extract('(C[0-9]+)')
    
    null_discount_count = df["discount"].isna().sum()
    df["discount"] = df["discount"].fillna(0)

    if null_discount_count > 0:
        logger.info(f"Filled NULL discounts with 0 for {null_discount_count} records")

    logger.info("Parsing sales date column")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    invalid_date_count = df["date"].isna().sum()

    if invalid_date_count > 0:
        logger.warning(
            f"Found {invalid_date_count} records with invalid or unparsable dates"
        )

    df["update_date"] = datetime.utcnow()

    # Data Quality checks
    df["dq_errors"] = ""

    df.loc[df["transaction_id"].isna(), "dq_errors"] += "Missing transaction_id; "
    df.loc[df["customer_id"].isna(), "dq_errors"] += "Missing customer_id; "
    df.loc[df["price"].isna(), "dq_errors"] += "Missing price; "
    df.loc[df["quantity"] <= 0, "dq_errors"] += "Invalid quantity; "
    
    df.loc[
        (df["discount"] < 0) | (df["discount"] > 1),
        "dq_errors"
    ] += "Invalid discount; "

    df["dq_errors"] = df["dq_errors"].str.rstrip("; ")
    
    invalid_mask = df["dq_errors"] != ""

    df_invalid = df[invalid_mask].copy()
    df_valid = df[~invalid_mask].copy()

    logger.info(
        f"Valid records: {len(df_valid)} | Invalid records: {len(df_invalid)}"
    )
    
    duration = (datetime.utcnow() - start_time).total_seconds()

    logger.info(
        f"Sales transformation completed in {duration:.2f} seconds"
    )

    return df_valid, df_invalid


def load_sales_to_silver(df, db_config):
    """
    Loads transformed sales data into the silver.sales table using
    an UPSERT strategy on transaction_id. Existing records are updated,
    and new records are inserted. Skips execution if no data is provided.
    """
    if df.empty:
        logger.info("No records to load into silver.sales. Skipping load step.")
        return

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    upsert_sql = """
        INSERT INTO silver.sales (
            transaction_id,
            customer_id,
            product_id,
            product_name,
            category,
            price,
            quantity,
            discount,
            date,
            region,
            insert_date,
            update_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (transaction_id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity,
            discount = EXCLUDED.discount,
            date = EXCLUDED.date,
            region = EXCLUDED.region,
            update_date = EXCLUDED.update_date;
    """

    try:
        records = [
            (
                row.transaction_id,
                row.customer_id,
                row.product_id,
                row.product_name,
                row.category,
                row.price,
                row.quantity,
                row.discount,
                row.date,
                row.region,
                row.insert_date,
                row.update_date
            )
            for row in df.itertuples(index=False)
        ]

        cursor.executemany(upsert_sql, records)
        conn.commit()

        logger.info(
            f"Loaded {len(records)} records into silver.sales "
            f"(UPSERT on transaction_id)"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed loading data into silver.sales: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def update_sales_etl_metadata(db_config):
    """
    Updates the ETL metadata table with the latest insert_date from
    raw.sales_raw for the sales_silver pipeline. This enables
    incremental processing in future runs.
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT MAX(insert_date)
            FROM raw.sales_raw
        """)
        max_insert_date = cursor.fetchone()[0]

        if not max_insert_date:
            logger.warning("No insert_date found in raw.sales_raw. Metadata not updated.")
            return

        cursor.execute("""
            INSERT INTO silver.etl_metadata (pipeline_name, last_insert_date)
            VALUES (%s, %s)
            ON CONFLICT (pipeline_name)
            DO UPDATE SET last_insert_date = EXCLUDED.last_insert_date
        """, ("sales_silver", max_insert_date))

        conn.commit()
        logger.info(
            f"Updated ETL metadata for sales_silver with last_insert_date = {max_insert_date}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update ETL metadata: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def make_json_safe(obj):
    """
    Recursively converts Python objects into JSON-serializable formats.
    Handles dictionaries, lists, pandas DataFrames, timestamps, dates,
    NaN values, and datetime objects to ensure safe JSON output.
    """
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]

    elif isinstance(obj, pd.DataFrame):
        return obj.map(make_json_safe).to_dict(orient="records")

    elif isinstance(obj, pd.Timestamp):
        if pd.isna(obj):
            return None
        return obj.strftime('%Y-%m-%d %H:%M:%S')

    elif isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')

    elif isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')

    elif isinstance(obj, float) and math.isnan(obj):
        return None

    else:
        return obj
