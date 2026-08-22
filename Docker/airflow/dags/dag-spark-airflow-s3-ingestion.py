# airflow/dags/dag-spark-airflow-s3-ingestion.py
from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "spark-airflow-s3-ingestion-dag"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_APP = "/opt/project/spark_processing/spark-stream-s3-aws.py"

PID_FILE = "/opt/airflow/logs/spark-airflow-s3-ingestion-dag.pid"
LOG_FILE = "/opt/airflow/logs/spark-airflow-s3-ingestion-dag.log"
STARTUP_WAIT_SECONDS = 30


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def start_spark_stream() -> None:
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r", encoding="utf-8") as f:
                pid = int(f.read().strip())
            if _pid_is_running(pid):
                print(f"Spark already running with PID {pid}")
                return
        except Exception:
            pass

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    cmd = [
        SPARK_SUBMIT,
        "--master", "spark://spark-master:7077",
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4",
        "--conf",
        "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2",
        "--conf",
        "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2",
        SPARK_APP,
    ]

    env = os.environ.copy()

    with open(LOG_FILE, "a", encoding="utf-8") as log:
        proc = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=log,
            env=env,
            start_new_session=True,
        )

    with open(PID_FILE, "w", encoding="utf-8") as f:
        f.write(str(proc.pid))

    time.sleep(STARTUP_WAIT_SECONDS)
    if proc.poll() is not None:
        raise AirflowFailException(f"Spark failed to start. Check {LOG_FILE}")

    print(f"Spark started successfully with PID {proc.pid}")


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
    tags=["spark", "s3", "ingestion", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_spark_stream = PythonOperator(
        task_id="run_spark_stream",
        python_callable=start_spark_stream,
    )

    validate_raw_landing = EmptyOperator(task_id="validate_raw_landing")

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation_pipeline",
        trigger_dag_id="dbt_transformation_pipeline",
        wait_for_completion=False,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> run_spark_stream >> validate_raw_landing >> trigger_dbt >> end