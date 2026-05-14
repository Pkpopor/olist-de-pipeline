import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv

PIPELINE_DB_CONN_ID = "pipeline_db"

PROJECT_ROOT = Path(__file__).parent.parent
DBT_DIR = "/opt/airflow/dbt"

load_dotenv(PROJECT_ROOT / ".env")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ingest.ingest import main as run_ingest

log = logging.getLogger(__name__)


def _on_failure_callback(context):
    ti = context.get("task_instance")
    exception = context.get("exception")
    log.error(
        "Task failed | dag=%s | task=%s | run_id=%s | execution_date=%s | error=%s",
        context.get("dag").dag_id,
        ti.task_id if ti else "unknown",
        context.get("run_id"),
        context.get("execution_date"),
        exception,
    )


def validate_quarantine(**context):
    conn = PostgresHook(postgres_conn_id=PIPELINE_DB_CONN_ID).get_conn()
    try:
        with conn.cursor() as cur:
            # Identify the latest run and its aggregate row counts
            cur.execute("""
                SELECT
                    run_id,
                    MIN(started_at)   AS started_at,
                    MAX(finished_at)  AS finished_at,
                    SUM(rows_total)   AS total_rows
                FROM raw.ingestion_log
                WHERE run_id = (
                    SELECT run_id
                    FROM   raw.ingestion_log
                    ORDER  BY started_at DESC
                    LIMIT  1
                )
                GROUP BY run_id
            """)
            row = cur.fetchone()

        if not row:
            log.warning("No ingestion_log records found; skipping quarantine check.")
            return

        run_id, started_at, finished_at, total_rows = row

        if not total_rows:
            log.warning("total_rows is 0 for run %s; skipping quarantine check.", run_id)
            return

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM   raw.quarantine
                WHERE  quarantined_at >= %s
                  AND  quarantined_at <= %s
                """,
                (started_at, finished_at),
            )
            quarantine_count = cur.fetchone()[0]

    finally:
        conn.close()

    pct = quarantine_count / total_rows * 100
    log.info(
        "Quarantine check | run_id=%s | quarantined=%d | total=%d | pct=%.2f%%",
        run_id, quarantine_count, total_rows, pct,
    )

    threshold = float(os.getenv("QUARANTINE_THRESHOLD_PCT", "5.0"))
    if pct > threshold:
        raise Exception(
            f"Quarantine rate {pct:.2f}% exceeds {threshold}% threshold "
            f"(quarantined={quarantine_count}, total={total_rows}, run_id={run_id})"
        )

    if quarantine_count > 0:
        log.warning(
            "Quarantine warning: %d rows (%.2f%%) were quarantined in the latest run.",
            quarantine_count,
            pct,
        )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=run_ingest,
    )

    validate_quarantine_task = PythonOperator(
        task_id="validate_quarantine",
        python_callable=validate_quarantine,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            'cd "$DBT_DIR" && '
            "dbt run --profiles-dir . --select staging.* --no-write-json && "
            "dbt test --profiles-dir . --select staging.* --no-write-json"
        ),
        env={**os.environ, "DBT_DIR": DBT_DIR},
    )

    dbt_mart = BashOperator(
        task_id="dbt_mart",
        bash_command=(
            'cd "$DBT_DIR" && '
            "dbt run --profiles-dir . --select marts.* --no-write-json && "
            "dbt test --profiles-dir . --select marts.* --no-write-json"
        ),
        env={**os.environ, "DBT_DIR": DBT_DIR},
    )

    ingest_raw >> validate_quarantine_task >> dbt_staging >> dbt_mart
