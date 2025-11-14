"""Airflow DAG that runs a basic dbt workflow (deps ➜ seed ➜ run ➜ test)."""
from __future__ import annotations

from datetime import datetime, timedelta
import os
import shlex

from airflow import DAG
from airflow.operators.bash import BashOperator

# Allow overriding the dbt project/profiles locations via environment variables mounted in Docker.
RAW_DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
RAW_DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", RAW_DBT_PROJECT_DIR)
DBT_PROJECT_DIR = shlex.quote(RAW_DBT_PROJECT_DIR)
DBT_PROFILES_DIR = shlex.quote(RAW_DBT_PROFILES_DIR)

DEFAULT_ARGS = {
    "owner": "analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_daily_refresh",
    description="Runs the dbt project end-to-end.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "warehouse"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test
