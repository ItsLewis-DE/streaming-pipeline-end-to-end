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
    schedule='*/5 * * * *',
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
    # -------------------------------------------------------------------------
    # Task 2: Chạy Spark Job Build Dimensions
    # -------------------------------------------------------------------------
    build_dimensions = make_spark_task(
        task_id='build_dimensions_job',
        script_path='/opt/spark/app/gold/build_dimensions.py',
    )

    # -------------------------------------------------------------------------
    # Task 3: Check DLQ bucket
    # -------------------------------------------------------------------------
    run_dlq_monitoring = make_spark_task(
        task_id='run_dlq_monitoring_job',
        script_path='/opt/spark/app/bronze/dlq_monitoring.py',
        extra_args=[
            '--threshold', '20.0',
            '--hours', '1'
        ]
    )
    run_dlq_monitoring >> run_bronze_to_silver >> build_dimensions
