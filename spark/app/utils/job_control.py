import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)
CONTROL_TABLE = "metadata.job_control"

def insert_log(spark: SparkSession, bucket_name: str, table_name: str, max_timestamp: str) -> bool:
    try:
        if not spark.catalog.tableExists(CONTROL_TABLE):
            spark.sql(f"""
                CREATE TABLE {CONTROL_TABLE} (bucket_name STRING, table_name STRING, max_timestamp STRING)
                USING iceberg
            """)
        _data = [
            [bucket_name, table_name, max_timestamp]
        ]
        _col = ["bucket_name", "table_name", "max_timestamp"]
        df_source = spark.createDataFrame(_data, _col)
        df_source.createOrReplaceTempView("_source")

        spark.sql(f"""
            MERGE INTO {CONTROL_TABLE} t
            USING _source s 
            ON t.bucket_name = s.bucket_name and t.table_name = s.table_name 
            WHEN MATCHED THEN UPDATE SET t.max_timestamp = s.max_timestamp
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        logger.info("Da insert thanh cong!")
        return True
    except Exception as e:
        logger.error(f"Loi khi upsert: {e}")
        return False

def get_max_timestamp(spark: SparkSession, bucket_name: str, table_name: str) -> str:
    """
    Lấy max_timestamp cho 1 cặp (bucket_name, table_name) từ bảng control.
    Trả về config mặc định nếu không tìm thấy hoặc bảng chưa tồn tại.
    """
    try:
        if spark.catalog.tableExists(CONTROL_TABLE):
            row = (
                spark.read.table(CONTROL_TABLE)
                .filter((col("bucket_name") == bucket_name) & (col("table_name") == table_name))
                .select(col("max_timestamp"))
                .first()
            )

            if row:
                return str(row["max_timestamp"])

        logger.warning(f"Không tìm thấy max_timestamp cho {bucket_name}.{table_name}")
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
