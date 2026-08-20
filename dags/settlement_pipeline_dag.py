"""
Airflow DAG: merchant settlement analytics pipeline.

Flow:  anonymize -> ingest -> dbt run -> dbt test -> freshness -> notify

Design decisions worth defending in review:

1. FAIL CLOSED. `dbt test` is a separate task downstream of `dbt run`, and the
   publish step depends on it. Tests are not advisory — if a control fails, nothing
   downstream is treated as trustworthy. This mirrors the failure mode the pipeline
   exists to prevent: analyses completing on data nobody validated.

2. IDEMPOTENT TASKS. Ingestion dedupes on row hash, so a retry after a partial
   failure cannot double-load. That is what makes automatic retries safe.

3. RETRIES ONLY ON TRANSIENT WORK. Ingest retries (a file lock or transient IO can
   resolve itself). dbt test does NOT retry — a failing control is a real result, and
   retrying it just delays the alert.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_DIR = os.environ.get("PIPELINE_HOME", "/opt/airflow/pipeline")
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

DEFAULT_ARGS = {
    "owner": "analytics",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.environ.get("ALERT_EMAIL", "analytics-alerts@example.com")],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="merchant_settlement_pipeline",
    description="Ingest merchant settlement export, transform with dbt, enforce data quality controls.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",          # daily 06:00, after the platform's overnight export
    catchup=False,
    max_active_runs=1,             # settlement data is cumulative; concurrent runs would race
    tags=["analytics", "settlement", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    anonymize = BashOperator(
        task_id="anonymize_source_data",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python ingestion/anonymize.py --src $SOURCE_EXPORT_DIR --dest data/anonymized"
        ),
    )

    ingest = BashOperator(
        task_id="ingest_to_raw",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python ingestion/ingest.py --src data/anonymized --db warehouse.duckdb"
        ),
    )

    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --target {DBT_TARGET}",
        retries=0,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --target {DBT_TARGET}",
        retries=1,
    )

    # No retries: a failing control is a result, not a flake.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --target {DBT_TARGET}",
        retries=0,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_DIR} && dbt docs generate --target {DBT_TARGET}",
        retries=0,
    )

    # Downstream of dbt_test, so nothing publishes unless the controls passed.
    publish = EmptyOperator(task_id="publish_marts")

    start >> anonymize >> ingest >> source_freshness >> dbt_run >> dbt_test >> [dbt_docs, publish]
