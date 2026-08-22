# airflow/dags/dag-dbt-transformation-pipeline.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "dbt_transformation_pipeline"
DBT_PROJECT_DIR = "/opt/project/dbt_project"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 3, 27),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dbt", "snowflake", "transformation"],
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_debug >> dbt_deps >> dbt_run >> dbt_test >> end