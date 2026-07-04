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
                    .appName("Building_dim_table") \
                    .config("spark.sql.shuffle.partitions", 16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()

from pyspark.sql.window import Window
from utils.job_control import get_max_timestamp, insert_log

def create_dim_tables(spark: SparkSession):
    #Partition theo is_current để lúc sau dùng where cho nhanh
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_user (
            user_pseudo_id STRING,
            level STRING,
            zip STRING,
            gender STRING,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            is_current BOOLEAN
        ) USING iceberg PARTITIONED BY (is_current) 
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_song (
            song_id STRING, song_name STRING, artist_name STRING
        ) USING iceberg
    """)

def load_dim_song(spark: SparkSession):
    try:
        job_name = "dim_song"
        max_ts = get_max_timestamp(spark, "gold", job_name)
        logger.info(f"Bơm dữ liệu dim_song (SCD1) từ {max_ts}...")
        
        df_listen = spark.table("lakehouse.silver.listen_events").where(f"_processed_at > '{max_ts}'")
        if df_listen.isEmpty():
            logger.info("Không có bài hát mới.")
            return

        # Trích xuất và băm MD5 để làm song_id
        df_new_songs = df_listen.filter(F.col("song").isNotNull() & F.col("artist").isNotNull()) \
            .select(
                F.md5(F.concat_ws("||", F.col("artist"), F.col("song"))).alias("song_id"),
                F.col("song").alias("song_name"),
                F.col("artist").alias("artist_name"),
                F.col("duration").alias("song_duration")
            ).dropDuplicates(["song_id"])
            
        if df_new_songs.isEmpty(): return
        
        df_new_songs.createOrReplaceTempView("vw_new_songs")
        
        # SCD1: Chỉ thêm mới, bỏ qua bài đã có
        spark.sql("""
            MERGE INTO lakehouse.gold.dim_song t
            USING vw_new_songs s
            ON t.song_id = s.song_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        new_max = df_listen.agg(F.max("_processed_at")).collect()[0][0]
        insert_log(spark, bucket_name, job_name, min_ts=max_ts, max_ts=new_max, row_count=df_new_songs.count(), status="SUCCESS")
    except:
        insert_log(
            spark=spark, 
            bucket_name=bucket_name, 
            table_name=job_name, 
            min_timestamp="", 
            max_timestamp="", 
            row_count=0, 
            status="FAILED",
            error_message=str(e)
        )
        raise e

def load_dim_user(spark: SparkSession):
    job_name = "dim_user"
    max_ts = get_max_timestamp(spark, "gold", job_name)
    logger.info(f"Bơm dữ liệu dim_user (SCD2) từ {max_ts}...")
    
    df_status = spark.table("lakehouse.silver.status_change_events").where(f"_processed_at > '{max_ts}'")
    
    if df_status.isEmpty():
        logger.info("Không có thay đổi user nào mới.")
        return
        
    # 1. Lấy trạng thái MỚI NHẤT của user (Bao gồm cả level và zip đã mask)
    w_status = Window.partitionBy("user_pseudo_id").orderBy(F.col("ts").desc())
    df_updates = df_status.withColumn("rn", F.row_number().over(w_status)) \
                          .where("rn = 1") \
                          .select("user_pseudo_id", "level", "zip","gender")
    
    # 2. Xử lý SCD2: Đóng cờ (is_current = false) cho lịch sử cũ
    df_updates = df_updates.withColumn("start_date", F.current_timestamp())
    df_updates.createOrReplaceTempView("vw_user_updates")
    
    spark.sql("""
        MERGE INTO lakehouse.gold.dim_user t
        USING vw_user_updates s
        ON t.user_pseudo_id = s.user_pseudo_id AND t.is_current = true
        WHEN MATCHED THEN
            UPDATE SET is_current = false, end_date = s.start_date
    """)
    
    # 3. Chèn bản ghi mới (is_current = true)
    spark.sql("""
        INSERT INTO lakehouse.gold.dim_user
        SELECT user_pseudo_id, level, zip, start_date, CAST(NULL AS TIMESTAMP) as end_date, true as is_current
        FROM vw_user_updates
    """)
    
    # Cập nhật Watermark Log
    new_max = df_status.agg(F.max("_processed_at")).collect()[0][0]
    insert_log(spark, "gold", job_name, min_ts=max_ts, max_ts=new_max, row_count=df_updates.count(), status="SUCCESS")

if __name__ == "__main__":
    create_dim_tables(spark)
    load_dim_song(spark)
    load_dim_user(spark)
    logger.info("Hoàn tất Job Build Dimensions!")
    spark.stop()