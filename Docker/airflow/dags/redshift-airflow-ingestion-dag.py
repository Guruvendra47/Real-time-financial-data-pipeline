# ==========================================
# IMPORT LIBRARIES
# ==========================================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

# ==========================================
# LOAD FUNCTION
# ==========================================
def load_to_redshift():
    """
    WHY:
    This function loads data from S3 into Redshift using COPY.
    Runs every time Airflow triggers the DAG.
    """

    # ==========================================
    # READ ENV VARIABLES (SECURITY BEST PRACTICE)
    # ==========================================
    host = os.getenv("REDSHIFT_HOST")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    dbname = os.getenv("REDSHIFT_DB")
    iam_role = os.getenv("REDSHIFT_IAM_ROLE")

    if not all([host, user, password, dbname, iam_role]):
        raise ValueError("Missing required environment variables")

    # ==========================================
    # CONNECT TO REDSHIFT
    # ==========================================
    conn = psycopg2.connect(
        host=host,
        port=5439,
        dbname=dbname,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    # ==========================================
    # CREATE TABLE IF NOT EXISTS
    # WHY:
    # Prevent pipeline failure on first run
    # ==========================================
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_stock_data (
        price FLOAT,
        symbol VARCHAR(50),
        timestamp TIMESTAMP,
        volume FLOAT
    );
    """

    cursor.execute(create_table_query)

    # ==========================================
    # COPY COMMAND (S3 → REDSHIFT)
    # WHY:
    # This is fastest and standard ingestion method
    # ==========================================
    copy_query = f"""
        COPY raw_stock_data
        FROM 's3://real-time-financial-data-pipeline/raw/trades/'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """

    cursor.execute(copy_query)
    conn.commit()

    print("✅ Data successfully loaded into Redshift")

    cursor.close()
    conn.close()


# ==========================================
# DEFAULT ARGS
# ==========================================
default_args = {
    'owner': 'airflow',

    # WHY:
    # If task fails → retry automatically
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

# ==========================================
# DAG DEFINITION
# ==========================================
dag = DAG(
    's3_to_redshift_pipeline',

    # WHY:
    # Prevent backfill of old runs
    catchup=False,

    # WHY:
    # Runs every 2 minutes → near real-time ingestion
    schedule_interval='*/2 * * * *',

    start_date=datetime(2026, 1, 1),

    default_args=default_args
)

# ==========================================
# TASK
# ==========================================
load_task = PythonOperator(
    task_id='copy_s3_to_redshift',

    # WHY:
    # Calls our ingestion function
    python_callable=load_to_redshift,

    dag=dag
)