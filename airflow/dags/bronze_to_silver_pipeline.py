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
    dag_id='bronze_to_silver_pipeline',
    default_args=default_args,
    description='Pipeline chạy định kỳ (Batch) làm sạch và khử trùng lặp dữ liệu từ Bronze sang Silver',
    schedule_interval='@hourly',
    start_date=datetime(2026, 6, 28),
    catchup=False, # Vì Spark script tự quản lý dữ liệu mới bằng bảng metadata.job_control, ta không cần catchup
    tags=['silver', 'batch', 'iceberg'],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Chạy Spark Job Bronze to Silver
    # Sử dụng hàm tiện ích `make_spark_task` để gọi qua SSH
    # -------------------------------------------------------------------------
    run_bronze_to_silver = make_spark_task(
        task_id='run_bronze_to_silver_job',
        script_path='/opt/spark/app/silver/bronze_to_silver.py',
        extra_args=[] # Script hiện tại tự tính toán ngày tháng theo job_control nên không cần truyền param --date
    )

    run_bronze_to_silver
