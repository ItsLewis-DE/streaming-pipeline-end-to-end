import sys 
import os
import logging
from pyspark.sql import functions as F 
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schemas import ALL_TOPICS
from utils.job_control import get_max_timestamp
# Cấu hình logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

spark = SparkSession.builder \
                    .appName("Check_silver_layer") \
                    .config("spark.sql.shuffle.partitions", 16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()
    
def check_silver(spark: SparkSession, bucket_name: str, table_name: str,min_row:int,max_null_row:int) -> None:
    check_table = f"lakehouse.{bucket_name}.{table_name}"
    if not spark.catalog.tableExists(check_table):
        logger.error(f"Khong tim thay bang {check_table}!")
        sys.exit(1)
    max_timestamp = get_max_timestamp(spark,bucket_name,table_name)
    df_source = spark.table(check_table).where(f"_processed_at >'{max_timestamp}'")
    so_dong = df_source.count()
    logger.info(f"Kiem tra bang {check_table} thanh cong, so dong moi: {so_dong}")
    if so_dong < min_row:
        logger.error(f"Số dòng mới ({so_dong}) ít hơn mức tối thiểu ({min_row}), xin hãy kiểm tra lại!")
        sys.exit(1)
    
    # Kiểm tra cột user_pseudo_id có tồn tại không trước khi đếm null
    if "user_pseudo_id" in df_source.columns:
        count_null = df_source.filter(F.col("user_pseudo_id").isNull()).count() 
        if count_null > max_null_row:
            logger.error(f"Số dòng null ({count_null}) vượt quá mức cho phép ({max_null_row}), xin hãy kiểm tra lại!")
            sys.exit(1)
