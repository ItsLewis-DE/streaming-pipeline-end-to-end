import os,json 
from datetime import timedelta 
from prometheus_client import Gauge,start_http_server 
from pyspark.sql import SparkSession,functions as F 
from pyspark.sql.streaming import StreamingQueryListener

PORT=int(os.getenv("PORT","9108"))
start_http_server(PORT)

TOTAL_ACTIVE_LISTENERS=Gauge("total_listeners","Total listeners is activating now")
PAID_USERS_RATIO=Gauge("paid_users_ratio","Percentage of VIP users is activating (%)")
AVG_SONG_DURATION = Gauge("avg_song_duration_seconds", "The average duration of songs")
TOTAL_SONG=Gauge("total_song","Total song is listened")

SPARK_CONSUMER_LAG = Gauge("spark_consumer_lag", "Spark maximum offsets behind latest")
SPARK_CONSUMER_RATE = Gauge("spark_consume_rate_per_minute", "Spark message consume rate per minute")

class PrometheusQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass
        
    def onQueryProgress(self, event):
        try:
            progress = event.progress
            if progress.sources:
                for source in progress.sources:
                    metrics = source.metrics
                    if metrics and "maxOffsetsBehindLatest" in metrics:
                        SPARK_CONSUMER_LAG.set(float(metrics["maxOffsetsBehindLatest"]))
            
            if progress.processedRowsPerSecond is not None:
                SPARK_CONSUMER_RATE.set(float(progress.processedRowsPerSecond) * 60)
        except Exception:
            pass
            
    def onQueryTerminated(self, event):
        pass
spark = SparkSession.builder \
        .appName("Stream metrics") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions",16) \
        .getOrCreate()

spark.streams.addListener(PrometheusQueryListener())

# Cấu hình maxOffsetsPerTrigger để Spark đọc từ từ, giúp Prometheus bắt kịp sự thay đổi
kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers","kafka-1:9092") \
            .option("subscribe","listen_events") \
            .option("startingOffsets","earliest") \
            .option("maxOffsetsPerTrigger", 200) \
            .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS v").select(
    F.get_json_object("v","$.artist").alias("artist"),
    F.get_json_object("v","$.duration").cast("double").alias("duration"),
    F.trim(F.lower(F.get_json_object("v","$.level"))).alias("level"),
    F.get_json_object("v","$.state").alias("state"),
    F.get_json_object("v","$.userId").alias("userId"),
    F.get_json_object("v","$.song").alias("song")
).withColumn("timestamp", F.current_timestamp())

#Mỗi 15s thì spark sẽ đi qua kafka_df để lấy dữ liệu từ kafka, sau đó qua các hàm rồi sẽ tới window_df nhưng vì ta để window là 5 minutes nên khi df này được gửi qua hàm upload thì df sẽ rỗng, chỉ khi tới phút thứ 6 thì dữ liệu mới đc gửi qua hàm upload

windowed_df = parsed_df.withWatermark("timestamp","1 minutes") \
                        .groupBy(F.window(F.col("timestamp"),"5 minutes","30 seconds")) \
                        .agg(
                            F.approx_count_distinct("userId").alias("active_user"), \
                            F.avg("duration").alias("avg_duration"), \
                            (F.sum(F.when(F.col("level")=="paid",1).otherwise(0)) / F.count("*") * 100).alias("paid_users_ratio"), \
                            F.approx_count_distinct("song").alias("total_songs")
                            ) \
                        .select(
                            F.col("window.start").alias("window_start"), \
                            F.col("window.end").alias("window_end"), \
                            "active_user", "avg_duration", "paid_users_ratio", "total_songs"
                        )

def update_prometheus(batch_df,batch_id):
    if batch_df.isEmpty():
        return 
    latest_end = batch_df.agg(F.max("window_end").alias("m")).collect()[0]["m"]
    latest = batch_df.filter(F.col("window_end")==F.lit(latest_end)) \
                    .limit(1) \
                    .collect()
    if not latest:
        return 
    row = latest[0]
    print(f"DEBUG BATCH {batch_id}: latest_end={latest_end}, row={row.asDict()}", flush=True)

    TOTAL_ACTIVE_LISTENERS.set(int(row["active_user"] or 0))
    PAID_USERS_RATIO.set(float(row["paid_users_ratio"] or 0))
    AVG_SONG_DURATION.set(float(row["avg_duration"] or 0))
    TOTAL_SONG.set(int(row["total_songs"] or 0))

#Mode update nay nghia la Spark se xuat ra trang thai hien tai cua window moi 15s thay vi cho window dong
windowed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(update_prometheus) \
    .option("checkpointLocation","/opt/spark/checkpoints/streams_metrics_v2") \
    .trigger(processingTime="15 seconds") \
    .start()

# Chờ streaming chạy vô hạn (Nếu không có dòng này, script sẽ chạy xong và thoát ngay lập tức)
spark.streams.awaitAnyTermination()