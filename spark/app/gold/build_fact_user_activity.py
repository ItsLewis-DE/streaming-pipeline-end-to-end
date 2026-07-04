import sys 
import os
import logging
from pyspark.sql import functions as F 
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.job_control import get_max_timestamp, insert_log

# Cấu hình logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

spark = SparkSession.builder \
                    .appName("Building_fact_user_activity_5min") \
                    .config("spark.sql.shuffle.partitions", 16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()

# ========================================================================
# TẠO BẢNG FACT
# ========================================================================
def create_fact_tables(spark: SparkSession):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_user_activity_5min (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            user_pseudo_id STRING,
            songs_played INT,
            listen_duration DOUBLE,
            page_views INT
        ) USING iceberg PARTITIONED BY (days(window_start))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_platform_health_5min (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            active_users INT,
            paid_users INT,
            free_users INT,
            total_listens INT,
            total_page_views INT,
            total_errors INT
        ) USING iceberg PARTITIONED BY (days(window_start))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_top_content_5min (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            song_id STRING,
            play_count INT
        ) USING iceberg PARTITIONED BY (days(window_start))
    """)
    logger.info("Đã khởi tạo các bảng Fact Gold!")

# ========================================================================
# FACT 1: USER ACTIVITY (Gộp 2 bảng Silver)
# ========================================================================
def load_fact_user_activity(spark: SparkSession):
    job_name = "fact_user_activity_5min"
    max_ts = get_max_timestamp(spark, "gold", job_name)
    logger.info(f"[{job_name}] Đọc dữ liệu mới từ {max_ts}...")

    try:
        # --- Đọc incremental từ 2 bảng Silver ---
        df_listen = spark.table("lakehouse.silver.listen_events").where(f"_processed_at > '{max_ts}'")
        df_page = spark.table("lakehouse.silver.page_view_events").where(f"_processed_at > '{max_ts}'")

        # Nếu không có dữ liệu mới ở cả 2 bảng thì dừng
        if df_listen.isEmpty() and df_page.isEmpty():
            logger.info(f"[{job_name}] Không có dữ liệu mới.")
            return

        # --- Gom nhóm riêng từng bảng theo cửa sổ 5 phút ---
        # Bảng Listen: đếm bài hát + tổng thời lượng nghe
        df_listen_agg = df_listen.groupBy(
            F.window(F.col("ts"), "5 minutes"),
            F.col("user_pseudo_id")
        ).agg(
            F.countDistinct("song").alias("songs_played"),
            F.sum("duration").alias("listen_duration")
        )

        # Bảng Page View: đếm số lần lướt trang
        df_page_agg = df_page.groupBy(
            F.window(F.col("ts"), "5 minutes"),
            F.col("user_pseudo_id")
        ).agg(
            F.count("*").alias("page_views")
        )

        # --- FULL OUTER JOIN 2 DataFrame ---
        df_joined = df_listen_agg.join(
            df_page_agg, ["window", "user_pseudo_id"], "outer"
        )

        # Fill null bằng 0
        df_result = df_joined.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("user_pseudo_id"),
            F.coalesce(F.col("songs_played"), F.lit(0)).alias("songs_played"),
            F.coalesce(F.col("listen_duration"), F.lit(0.0)).alias("listen_duration"),
            F.coalesce(F.col("page_views"), F.lit(0)).alias("page_views")
        )

        # --- Append vào bảng Iceberg ---
        target_table = "lakehouse.gold.fact_user_activity_5min"
        row_count = df_result.count()
        if row_count > 0:
            df_result.writeTo(target_table).append()
            
            # Lấy max _processed_at từ cả 2 bảng để cập nhật watermark
            max_timestamps = []
            if not df_listen.isEmpty():
                max_timestamps.append(df_listen.agg(F.max("_processed_at")).collect()[0][0])
            if not df_page.isEmpty():
                max_timestamps.append(df_page.agg(F.max("_processed_at")).collect()[0][0])
            new_max = str(max(max_timestamps))

            insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=new_max, row_count=row_count, status="SUCCESS")
            logger.info(f"[{job_name}] Đã ghi {row_count} dòng thành công!")

    except Exception as e:
        logger.error(f"[{job_name}] Lỗi: {e}")
        insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=max_ts, row_count=0, status="FAILED", error_message=str(e))
        raise e

# ========================================================================
# FACT 2: PLATFORM HEALTH (KPI tổng hợp nền tảng, không theo user)
# ========================================================================
def load_fact_platform_health(spark: SparkSession):
    job_name = "fact_platform_health_5min"
    max_ts = get_max_timestamp(spark, "gold", job_name)
    logger.info(f"[{job_name}] Đọc dữ liệu mới từ {max_ts}...")

    try:
        # Bảng page_view_events chứa toàn bộ request HTTP (kể cả khi user bấm NextSong để nghe nhạc)
        df_page = spark.table("lakehouse.silver.page_view_events").where(f"_processed_at > '{max_ts}'")

        if df_page.isEmpty():
            logger.info(f"[{job_name}] Không có dữ liệu mới.")
            return

        df_result = df_page.groupBy(
            F.window(F.col("ts"), "5 minutes")
        ).agg(
            F.countDistinct("user_pseudo_id").alias("active_users"),
            F.countDistinct(F.when(F.col("level") == "paid", F.col("user_pseudo_id"))).alias("paid_users"),
            F.countDistinct(F.when(F.col("level") == "free", F.col("user_pseudo_id"))).alias("free_users"),
            F.sum(F.when(F.col("page") == "NextSong", 1).otherwise(0)).alias("total_listens"),
            F.count("*").alias("total_page_views"),
            F.sum(F.when(F.col("status") >= 400, 1).otherwise(0)).alias("total_errors")
        ).select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "active_users", "paid_users", "free_users",
            "total_listens", "total_page_views", "total_errors"
        )

        target_table = "lakehouse.gold.fact_platform_health_5min"
        row_count = df_result.count()
        if row_count > 0:
            df_result.writeTo(target_table).append()

            new_max = str(df_page.agg(F.max("_processed_at")).collect()[0][0])
            insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=new_max, row_count=row_count, status="SUCCESS")
            logger.info(f"[{job_name}] Đã ghi {row_count} dòng thành công!")

    except Exception as e:
        logger.error(f"[{job_name}] Lỗi: {e}")
        insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=max_ts, row_count=0, status="FAILED", error_message=str(e))
        raise e

# ========================================================================
# FACT 3: TOP CONTENT (Bảng xếp hạng bài hát Trending)
# ========================================================================
def load_fact_top_content(spark: SparkSession):
    job_name = "fact_top_content_5min"
    max_ts = get_max_timestamp(spark, "gold", job_name)
    logger.info(f"[{job_name}] Đọc dữ liệu mới từ {max_ts}...")

    try:
        df_listen = spark.table("lakehouse.silver.listen_events").where(f"_processed_at > '{max_ts}'")

        if df_listen.isEmpty():
            logger.info(f"[{job_name}] Không có dữ liệu mới.")
            return

        # Chỉ lấy những bản ghi có đầy đủ song + artist
        df_result = df_listen.filter(
            F.col("song").isNotNull() & F.col("artist").isNotNull()
        ).withColumn(
            "song_id", F.md5(F.concat_ws("||", F.col("artist"), F.col("song")))
        ).groupBy(
            F.window(F.col("ts"), "5 minutes"),
            "song_id"
        ).agg(
            F.count("*").alias("play_count")
        ).select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "song_id",
            "play_count"
        )

        target_table = "lakehouse.gold.fact_top_content_5min"
        row_count = df_result.count()
        if row_count > 0:
            df_result.writeTo(target_table).append()

            new_max = str(df_listen.agg(F.max("_processed_at")).collect()[0][0])
            insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=new_max, row_count=row_count, status="SUCCESS")
            logger.info(f"[{job_name}] Đã ghi {row_count} dòng thành công!")

    except Exception as e:
        logger.error(f"[{job_name}] Lỗi: {e}")
        insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=max_ts, row_count=0, status="FAILED", error_message=str(e))
        raise e

# ========================================================================
# MAIN
# ========================================================================
if __name__ == "__main__":
    create_fact_tables(spark)
    load_fact_user_activity(spark)
    load_fact_platform_health(spark)
    load_fact_top_content(spark)
    logger.info("Hoàn tất Job Build Facts 5-min!")
    spark.stop()
