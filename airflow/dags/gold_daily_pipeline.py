from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Thông số mặc định cho DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gold_daily_user_activity_pipeline',
    default_args=default_args,
    description='Pipeline chạy Batch hàng ngày chuyển dữ liệu từ Silver lên Gold',
    schedule_interval='@daily', # Chạy vào lúc 00:00 mỗi ngày
    start_date=datetime(2026, 6, 28), # Cấu hình ngày bắt đầu để hỗ trợ backfill
    catchup=True, # Cho phép Airflow chạy bù các ngày cũ bị lỡ
    tags=['gold', 'daily', 'batch', 'iceberg'],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Chạy Spark Batch Job trong container spark-master
    # Sử dụng `docker exec` thông qua BashOperator (yêu cầu mount docker.sock)
    # Tham số `{{ ds }}` là execution_date của Airflow, định dạng YYYY-MM-DD
    # -------------------------------------------------------------------------
    run_spark_batch = BashOperator(
        task_id='run_daily_user_activity_batch',
        bash_command=(
            "docker exec spark-master spark-submit "
            "/opt/spark/apps/gold/daily_user_activity_batch.py "
            "--date {{ ds }}"
        )
    )

    # Nếu có nhiều bảng Gold khác, bạn có thể định nghĩa thêm các BashOperator 
    # và cấu trúc luồng chạy ở đây (ví dụ: task1 >> task2)
    
    run_spark_batch
