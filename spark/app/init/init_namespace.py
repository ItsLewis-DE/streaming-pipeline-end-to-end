import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("init_namespace")
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("init_namespace") \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for namespace in ['sandbox','gold','bronze','silver', 'metadata']:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace} LOCATION 's3a://{namespace}/'")

logger.info("Đã tạo thành công các bucket")
