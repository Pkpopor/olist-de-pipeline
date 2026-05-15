import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
from contracts import TABLE_CONTRACTS, TableContract

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CSV_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))

_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                        VARCHAR(50) PRIMARY KEY,
    customer_id                     VARCHAR(50) NOT NULL,
    order_status                    VARCHAR(30),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    _ingested_at                    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id            VARCHAR(50)     NOT NULL,
    order_item_id       INT             NOT NULL,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    _ingested_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id              VARCHAR(50) PRIMARY KEY,
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix VARCHAR(64),
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(10),
    _ingested_at             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id                  VARCHAR(50) PRIMARY KEY,
    product_category_name       VARCHAR(100),
    product_name_lenght         INT,
    product_description_lenght  INT,
    product_photos_qty          INT,
    product_weight_g            NUMERIC(10,2),
    product_length_cm           NUMERIC(10,2),
    product_height_cm           NUMERIC(10,2),
    product_width_cm            NUMERIC(10,2),
    _ingested_at                TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id              VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(64),
    seller_city            VARCHAR(100),
    seller_state           VARCHAR(10),
    _ingested_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id             VARCHAR(50) NOT NULL,
    payment_sequential   INT         NOT NULL,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        NUMERIC(10,2),
    _ingested_at         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id               VARCHAR(50) PRIMARY KEY,
    order_id                VARCHAR(50) NOT NULL,
    review_score            INT,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    _ingested_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code_prefix VARCHAR(10)     PRIMARY KEY,
    geolocation_lat             NUMERIC(18, 15),
    geolocation_lng             NUMERIC(18, 15),
    geolocation_city            VARCHAR(100),
    geolocation_state           VARCHAR(10),
    _ingested_at                TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.product_category_name_translation (
    product_category_name         VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100),
    _ingested_at                  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.quarantine (
    id           SERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    row_data     JSONB,
    reason       TEXT,
    quarantined_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id               SERIAL PRIMARY KEY,
    run_id           VARCHAR(36),
    source_file      VARCHAR(200),
    target_table     VARCHAR(100),
    rows_total       INT,
    rows_inserted    INT,
    rows_quarantined INT,
    started_at       TIMESTAMP,
    finished_at      TIMESTAMP,
    status           VARCHAR(20),
    error_message    TEXT
);
"""

_NULL_STRINGS = {"", "nan", "none", "null", "na", "n/a"}


def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("PIPELINE_DB_HOST", "localhost"),
        port=int(os.getenv("PIPELINE_DB_PORT", 5433)),
        dbname=os.getenv("PIPELINE_DB_NAME", "raw"),
        user=os.getenv("PIPELINE_DB_USER"),
        password=os.getenv("PIPELINE_DB_PASSWORD"),
    )


def _create_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
    conn.commit()
    log.info("Schema and tables verified")


def _hash(value) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()


def _cast(value, dtype: str, nullable: bool):
    """Returns (casted_value, error_message_or_None)."""
    is_null = value is None or str(value).strip().lower() in _NULL_STRINGS
    if is_null:
        if not nullable:
            return None, "required field is null/empty"
        return None, None

    raw = str(value).strip()
    try:
        if dtype == "str":
            return raw, None
        if dtype == "int":
            return int(float(raw)), None
        if dtype == "float":
            return float(raw), None
        if dtype == "datetime":
            return pd.to_datetime(raw).to_pydatetime(), None
    except (ValueError, TypeError) as exc:
        return None, f"cannot cast '{value}' to {dtype}: {exc}"

    return raw, None


def _validate_row(row: dict, contract: TableContract):
    """Returns (transformed_dict, None) or (None, reason_str)."""
    errors = []
    transformed = {}

    for col in contract.columns:
        casted, err = _cast(row.get(col.name), col.dtype, col.nullable)
        if err:
            errors.append(f"{col.name}: {err}")
        else:
            transformed[col.name] = _hash(casted) if col.name in contract.pii_fields else casted

    if errors:
        return None, "; ".join(errors)
    return transformed, None


def _ingest_file(conn, contract: TableContract, run_id: str) -> None:
    csv_path = CSV_DIR / contract.source_file
    started_at = datetime.now(timezone.utc)
    status = "success"
    error_message = None
    rows_total = rows_inserted = rows_quarantined = 0

    try:
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
        rows_total = len(df)

        missing = {c.name for c in contract.columns} - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        valid_rows, quarantine_rows = [], []

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            transformed, reason = _validate_row(row_dict, contract)
            if reason:
                quarantine_rows.append((contract.target_table, json.dumps(row_dict, default=str), reason))
            else:
                valid_rows.append(transformed)

        col_names = [c.name for c in contract.columns]
        now = datetime.now(timezone.utc)

        if valid_rows:
            pk = ", ".join(contract.primary_key)
            sql = (
                f"INSERT INTO raw.{contract.target_table} "
                f"({', '.join(col_names)}, _ingested_at) VALUES %s "
                f"ON CONFLICT ({pk}) DO NOTHING"
            )
            values = [tuple(r[c] for c in col_names) + (now,) for r in valid_rows]
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
            rows_inserted = len(valid_rows)

        if quarantine_rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO raw.quarantine (source_table, row_data, reason) VALUES %s",
                    quarantine_rows,
                )
            rows_quarantined = len(quarantine_rows)

        conn.commit()
        log.info(
            "%-45s  inserted=%d  quarantined=%d  total=%d",
            contract.target_table, rows_inserted, rows_quarantined, rows_total,
        )

    except Exception as exc:
        conn.rollback()
        status = "error"
        error_message = str(exc)
        log.error("Error ingesting %s: %s", contract.source_file, exc)

    finished_at = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.ingestion_log
                (run_id, source_file, target_table, rows_total, rows_inserted,
                 rows_quarantined, started_at, finished_at, status, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (run_id, contract.source_file, contract.target_table,
             rows_total, rows_inserted, rows_quarantined,
             started_at, finished_at, status, error_message),
        )
    conn.commit()


def main() -> None:
    run_id = str(uuid.uuid4())
    log.info("Run %s started", run_id)

    conn = _get_connection()
    try:
        _create_tables(conn)
        for contract in TABLE_CONTRACTS:
            _ingest_file(conn, contract, run_id)
    finally:
        conn.close()

    log.info("Run %s finished", run_id)


if __name__ == "__main__":
    main()
