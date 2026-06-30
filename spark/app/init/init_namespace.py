import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger("init_namespace")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.spark_utils import get_spark_session
spark = get_spark_session("init_namespace")

spark.sparkContext.setLogLevel("WARN")

for namespace in ['sandbox','gold','bronze','silver']:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace} LOCATION 's3a://{namespace}/'")

logger.info("Đã tạo thành công các bucket")
