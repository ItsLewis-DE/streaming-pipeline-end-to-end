from airflow import DAG
from spark_config import make_spark_task
from datetime import datetime, timedelta

# Thông số mặc định cho DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='silver_to_gold_pipeline',
    default_args=default_args,
    description='Pipeline chạy định kỳ (Batch) xử lý dữ liệu từ Silver sang Gold',
    schedule='*/5 * * * *',
    start_date=datetime(2026, 6, 28),
    catchup=False,
    tags=['gold', 'batch', 'iceberg'],
) as dag:
    # -------------------------------------------------------------------------
    # Task 1: Chạy Spark Job Build Fact User Activity
    # -------------------------------------------------------------------------
    build_fact_user_activity = make_spark_task(
        task_id='build_fact_user_activity_job',
        script_path='/opt/spark/app/gold/build_fact_user_activity.py',
    )
    
    # -------------------------------------------------------------------------
    # Task 2: Export Data Sang Postgres
    # -------------------------------------------------------------------------
    export_to_postgres = make_spark_task(
        task_id='export_to_postgres_job',
        script_path='/opt/spark/app/gold/export_to_postgres.py',
        spark_conf=['--packages', 'org.postgresql:postgresql:42.5.4']
    )

    # Thiết lập dependencies
    build_fact_user_activity >> export_to_postgres
