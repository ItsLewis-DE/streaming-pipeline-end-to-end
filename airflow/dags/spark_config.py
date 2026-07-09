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
    "--deploy-mode", "client",
    "--conf", "spark.driver.memory=512m",
    "--conf", "spark.executor.memory=1536m",
    "--conf", "spark.executor.memoryOverhead=512m",
    "--conf", "spark.executor.cores=1",
    "--conf", "spark.cores.max=1",
    "--conf", "spark.sql.iceberg.vectorization.enabled=false"
]

def make_spark_task(task_id: str, script_path: str, spark_conf: Optional[List[str]] = None, extra_args: Optional[List[str]] = None) -> SSHOperator:
    args = extra_args or []
    conf = spark_conf or []
    
    # Xây dựng command spark-submit
    command_parts = [
        "export JAVA_HOME=/opt/java/openjdk &&",
        "export SPARK_HOME=/opt/spark &&",
        "export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH &&",
        SPARK_SUBMIT,
        *COMMON_CONF,
        *conf,
        script_path,
        *args
    ]
    
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook,
        command=" ".join(command_parts),
        cmd_timeout=7200
    )
