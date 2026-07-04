import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
CONTROL_TABLE = "metadata.job_run_history"

def get_job_control_schema():
    return StructType([
        StructField("bucket_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("min_timestamp", StringType(), True),
        StructField("max_timestamp", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("processed_at", TimestampType(), True)
    ])

def insert_log(spark: SparkSession, 
               bucket_name: str, 
               table_name: str, 
               min_timestamp: str, 
               max_timestamp: str, 
               row_count: int, 
               status: str, 
               error_message: str = None) -> bool:
    try:
        if not spark.catalog.tableExists(CONTROL_TABLE):
            spark.sql(f"""
                CREATE TABLE {CONTROL_TABLE} (
                    bucket_name STRING, 
                    table_name STRING, 
                    min_timestamp STRING,
                    max_timestamp STRING,
                    row_count BIGINT,
                    status STRING,
                    error_message STRING,
                    processed_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (days(processed_at))
            """)
        
        now = datetime.now()
        
        _data = [{
            "bucket_name": bucket_name,
            "table_name": table_name,
            "min_timestamp": min_timestamp,
            "max_timestamp": max_timestamp,
            "row_count": row_count,
            "status": status,
            "error_message": error_message,
            "processed_at": now
        }]
        
        df_source = spark.createDataFrame(_data, schema=get_job_control_schema())
        
        df_source.writeTo(CONTROL_TABLE).append()
        
        logger.info(f"Da insert log {status} thanh cong cho {bucket_name}.{table_name}!")
        return True
    except Exception as e:
        logger.error(f"Loi khi insert log vao job_control: {e}")
        return False

def get_max_timestamp(spark: SparkSession, bucket_name: str, table_name: str) -> str:
    """
    Lấy max_timestamp của lần chạy SUCCESS gần nhất cho 1 cặp (bucket_name, table_name) từ bảng history.
    Trả về config mặc định nếu không tìm thấy hoặc bảng chưa tồn tại.
    """
    try:
        if spark.catalog.tableExists(CONTROL_TABLE):
            row = (
                spark.read.table(CONTROL_TABLE)
                .filter((F.col("bucket_name") == bucket_name) & 
                        (F.col("table_name") == table_name) & 
                        (F.col("status") == "SUCCESS"))
                .orderBy(F.col("processed_at").desc())
                .select(F.col("max_timestamp"))
                .first()
            )

            if row:
                return str(row["max_timestamp"])

        logger.warning(f"Không tìm thấy max_timestamp SUCCESS cho {bucket_name}.{table_name}")
        logger.info("se doc datetime tu file run_config")
        
        try:
            with open("/opt/spark/app/utils/run_config.json", "r") as f:
                time_val = json.load(f)['max_timestamp']
            return time_val
        except Exception as e:
            return "1900-02-03 00:00:00.000"

    except Exception as e:
        logger.error(f"Lỗi khi lấy max_timestamp cho {bucket_name}.{table_name}: {e}")
        return "1900-02-03 00:00:00.000"
