import logging
import sys
import os
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Cấu hình logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

spark = SparkSession.builder \
                    .appName("ExportToPostgres") \
                    .config("spark.sql.shuffle.partitions", 16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()

JDBC_URL = "jdbc:postgresql://postgres-gold:5432/gold_db"
JDBC_PROPERTIES = {
    "user": "gold_user",
    "password": "gold_password",
    "driver": "org.postgresql.Driver"
}

def export_table(spark: SparkSession, db_name: str, table_name: str):
    full_table_name = f"lakehouse.{db_name}.{table_name}"
    try:
        if not spark.catalog.tableExists(full_table_name):
            logger.warning(f"Bảng {full_table_name} không tồn tại. Bỏ qua.")
            return

        df = spark.table(full_table_name)
        row_count = df.count()
        if row_count == 0:
            logger.info(f"Bảng {full_table_name} không có dữ liệu. Bỏ qua.")
            return
            
        logger.info(f"Đang export bảng {full_table_name} ({row_count} dòng) sang Postgres...")
        # Ghi đè toàn bộ dữ liệu (Overwrite) theo yêu cầu
        df.write.jdbc(url=JDBC_URL, table=table_name, mode="overwrite", properties=JDBC_PROPERTIES)
        logger.info(f"Đã export bảng {table_name} thành công!")
    except Exception as e:
        logger.error(f"Lỗi khi export bảng {full_table_name}: {str(e)}")

if __name__ == "__main__":
    tables_to_export = [
        "dim_song",
        "dim_user",
        "fact_user_activity_5min",
        "fact_platform_health_5min",
        "fact_top_content_5min"
    ]

    for t in tables_to_export:
        export_table(spark, "gold", t)

    logger.info("Hoàn tất Job Export To Postgres!")
    spark.stop()
