from airflow import DAG
from spark_config import make_spark_task
from datetime import datetime, timedelta

# Thông số mặc định cho DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['thanhphong'], # Điền địa chỉ email của bạn hoặc Admin vào đây
    'email_on_failure': True, # Bật email alert khi DAG bị fail
    'email_on_retry': False,
    'retries': 0, # Không cần retry vì nếu data lỗi thì phải sửa code/schema trước
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dlq_monitoring_pipeline',
    default_args=default_args,
    description='Pipeline chạy định kỳ (1 tiếng/lần) để giám sát bảng DLQ và cảnh báo lỗi Data Quality',
    schedule_interval='@hourly',
    start_date=datetime(2026, 6, 28),
    catchup=False, 
    tags=['bronze', 'monitoring', 'dlq', 'alert'],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Chạy Spark Job DLQ Monitoring
    # Script sẽ tự động raise error (sys.exit(1)) nếu Error Rate > Threshold (5%)
    # Việc này sẽ làm Task bị đánh dấu FAILED, từ đó kích hoạt Email/Alert của Airflow
    # -------------------------------------------------------------------------
    run_dlq_monitoring = make_spark_task(
        task_id='run_dlq_monitoring_job',
        script_path='/opt/spark/app/bronze/dlq_monitoring.py',
        extra_args=[
            '--threshold', '5.0',
            '--hours', '1'
        ]
    )

    run_dlq_monitoring
