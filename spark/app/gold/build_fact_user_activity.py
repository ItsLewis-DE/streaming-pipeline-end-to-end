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
    job_name_listen = "fact_user_activity_5min_listen"
    job_name_page = "fact_user_activity_5min_page"
    max_ts_listen = get_max_timestamp(spark, "gold", job_name_listen)
    max_ts_page = get_max_timestamp(spark, "gold", job_name_page)
    logger.info(f"Đọc listen > {max_ts_listen}, page > {max_ts_page}")

    try:
        # --- Đọc incremental từ 2 bảng Silver với 2 watermark riêng biệt ---
        df_listen = spark.table("lakehouse.silver.listen_events").where(f"_processed_at > '{max_ts_listen}'")
        df_page = spark.table("lakehouse.silver.page_view_events").where(f"_processed_at > '{max_ts_page}'")

        # Nếu không có dữ liệu mới ở cả 2 bảng thì dừng
        if df_listen.isEmpty() and df_page.isEmpty():
            logger.info(f"Không có dữ liệu mới cho fact_user_activity.")
            return

        # --- Gom nhóm riêng từng bảng theo cửa sổ 5 phút ---
        # Bảng Listen: đếm bài hát + tổng thời lượng nghe
        df_listen_agg = df_listen.groupBy(
            F.window(F.col("ingested_at"), "5 minutes"),
            F.col("user_pseudo_id")
        ).agg(
            F.countDistinct("song").alias("songs_played"),
            F.sum("duration").alias("listen_duration")
        )

        # Bảng Page View: đếm số lần lướt trang
        df_page_agg = df_page.groupBy(
            F.window(F.col("ingested_at"), "5 minutes"),
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

        # --- Dùng MERGE INTO thay vì Append để cộng dồn/upsert vào bảng Iceberg ---
        target_table = "lakehouse.gold.fact_user_activity_5min"
        row_count = df_result.count()
        if row_count > 0:
            df_result.createOrReplaceTempView("vw_fact_user_activity")
            spark.sql(f"""
                MERGE INTO {target_table} t
                USING vw_fact_user_activity s
                ON t.window_start = s.window_start 
                   AND t.user_pseudo_id = s.user_pseudo_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        songs_played = t.songs_played + s.songs_played,
                        listen_duration = t.listen_duration + s.listen_duration,
                        page_views = t.page_views + s.page_views
                WHEN NOT MATCHED THEN
                    INSERT (window_start, window_end, user_pseudo_id, songs_played, listen_duration, page_views)
                    VALUES (s.window_start, s.window_end, s.user_pseudo_id, s.songs_played, s.listen_duration, s.page_views)
            """)
            
            # Ghi log watermark TÁCH BIỆT
            if not df_listen.isEmpty():
                new_max_listen = str(df_listen.agg(F.max("_processed_at")).collect()[0][0])
                insert_log(spark, "gold", job_name_listen, min_timestamp=max_ts_listen, max_timestamp=new_max_listen, row_count=row_count, status="SUCCESS")
            
            if not df_page.isEmpty():
                new_max_page = str(df_page.agg(F.max("_processed_at")).collect()[0][0])
                insert_log(spark, "gold", job_name_page, min_timestamp=max_ts_page, max_timestamp=new_max_page, row_count=row_count, status="SUCCESS")
                
            logger.info(f"Đã MERGE {row_count} dòng thành công vào fact_user_activity!")

    except Exception as e:
        logger.error(f"Lỗi: {e}")
        insert_log(spark, "gold", "fact_user_activity_5min_error", min_timestamp="0", max_timestamp="0", row_count=0, status="FAILED", error_message=str(e))
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
            F.window(F.col("ingested_at"), "5 minutes")
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
            df_result.createOrReplaceTempView("vw_fact_platform_health")
            spark.sql(f"""
                MERGE INTO {target_table} t
                USING vw_fact_platform_health s
                ON t.window_start = s.window_start
                WHEN MATCHED THEN
                    UPDATE SET
                        active_users = t.active_users + s.active_users,
                        paid_users = t.paid_users + s.paid_users,
                        free_users = t.free_users + s.free_users,
                        total_listens = t.total_listens + s.total_listens,
                        total_page_views = t.total_page_views + s.total_page_views,
                        total_errors = t.total_errors + s.total_errors
                WHEN NOT MATCHED THEN
                    INSERT *
            """)

            new_max = str(df_page.agg(F.max("_processed_at")).collect()[0][0])
            insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=new_max, row_count=row_count, status="SUCCESS")
            logger.info(f"[{job_name}] Đã MERGE {row_count} dòng thành công!")

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
            F.window(F.col("ingested_at"), "5 minutes"),
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
            df_result.createOrReplaceTempView("vw_fact_top_content")
            spark.sql(f"""
                MERGE INTO {target_table} t
                USING vw_fact_top_content s
                ON t.window_start = s.window_start AND t.song_id = s.song_id
                WHEN MATCHED THEN
                    UPDATE SET play_count = t.play_count + s.play_count
                WHEN NOT MATCHED THEN
                    INSERT *
            """)

            new_max = str(df_listen.agg(F.max("_processed_at")).collect()[0][0])
            insert_log(spark, "gold", job_name, min_timestamp=max_ts, max_timestamp=new_max, row_count=row_count, status="SUCCESS")
            logger.info(f"[{job_name}] Đã MERGE {row_count} dòng thành công!")

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
