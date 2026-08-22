from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ==========================================
# DEFAULT CONFIG
# ==========================================
default_args = {
    'owner': 'guruvendra',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# ==========================================
# DAG
# ==========================================
dag = DAG(
    'snowflake_realtime_pipeline',
    default_args=default_args,
    description='End-to-end Kafka → Spark → Snowflake → dbt pipeline',
    schedule_interval='*/5 * * * *',   # every 5 mins
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# ==========================================
# TASK 1 — SPARK STREAMING JOB
# ==========================================
run_spark = BashOperator(
    task_id='run_spark',
    bash_command="""
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
/opt/project/spark_processing/spark-stream-s3-aws.py
""",
    dag=dag
)

# ==========================================
# TASK 2 — DBT RUN
# ==========================================
run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command="""
cd /opt/project/snowflake_project && dbt run
""",
    dag=dag
)

# ==========================================
# TASK 3 — DBT TEST
# ==========================================
test_dbt = BashOperator(
    task_id='test_dbt',
    bash_command="""
cd /opt/project/snowflake_project && dbt test
""",
    dag=dag
)

# ==========================================
# PIPELINE FLOW
# ==========================================
run_spark >> run_dbt >> test_dbt

