# ==========================================
# IMPORTS
# ==========================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ==========================================
# DEFAULT SETTINGS
# ==========================================
default_args = {
    'owner': 'guruvendra',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id='spark_streaming_pipeline',
    default_args=default_args,
    description='Kafka → Spark → Postgres Pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # ==========================================
    # TASK: RUN SPARK JOB
    # ==========================================
    run_spark_job = BashOperator(
        task_id='run_spark_streaming_job',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
        --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
        --conf spark.executor.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
        /opt/project/spark_processing/spark_streaming.py
        """
    )