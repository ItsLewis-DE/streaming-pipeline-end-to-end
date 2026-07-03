from typing import List, Optional
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

# Tạo kết nối SSH tới container spark-master
ssh_hook = SSHHook(
    remote_host="spark-master",
    username="sparkuser",
    password="sparkpass",
    port=22,
    cmd_timeout=7200
)

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

# Cấu hình chung cho tất cả các Spark Job trong pipeline
COMMON_CONF = [
    "--master", "spark://spark-master:7077",
    "--deploy-mode", "client"
    # "--conf", "spark.executor.memory=1g",
    # "--conf", "spark.executor.cores=1",
    # "--conf", "spark.cores.max=4",
]

def make_spark_task(task_id: str, script_path: str, extra_args: Optional[List[str]] = None) -> SSHOperator:
    """
    Hàm tiện ích tạo SSHOperator để chạy lệnh spark-submit trên spark-master.
    
    :param task_id: Tên task trong DAG
    :param script_path: Đường dẫn tuyệt đối tới file python script trên spark-master
    :param extra_args: Danh sách các tham số thêm truyền vào script (ví dụ: ['--date', '{{ ds }}'])
    """
    args = extra_args or []
    
    # Xây dựng command spark-submit
    command_parts = [
        "export JAVA_HOME=/opt/java/openjdk &&",
        "export SPARK_HOME=/opt/spark &&",
        "export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH &&",
        SPARK_SUBMIT,
        *COMMON_CONF,
        script_path,
        *args
    ]
    
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook,
        command=" ".join(command_parts),
        cmd_timeout=7200
    )
