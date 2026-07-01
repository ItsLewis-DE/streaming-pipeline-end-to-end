import os,json 
from datetime import timedelta 
from prometheus_client import Gauge,start_http_server 
from pyspark.sql import SparkSession,functions as F 

PORT=int(os.getenv("PORT","9108"))
start_http_server(PORT)

TOTAL_ACTIVE_LISTENERS=Gauge("total_listeners","Total listeners is activating now")
PAID_USERS_RATIO=Gauge("paid_users_ratio","Percentage of VIP users is activating (%)")
AVG_SONG_DURATION = Gauge("avg_song_duration_seconds", "The average duration of songs")
TOTAL_SONG=Gauge("total_song","Total song is listened")

spark = SparkSession.builder \
        .appName("Stream metrics") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions",16) \
        .getOrCreate()

#Vì sẽ lấy dữ liệu mới nhất nên sẽ để là latest thay vì dùng checkpoint
kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers","kafka-1:9092") \
            .option("subscribe","listen_events") \
            .option("startingOffsets","latest") \
            .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS v").select(
    F.get_json_object("v","$.artist").alias("artist"),
    F.get_json_object("v","$.duration").alias("duration"),
    F.get_json_object("v","$.level").alias("level"),
    F.get_json_object("v","$.state").alias("state"),
    F.get_json_object("v","$.userId").alias("userId"),
    F.get_json_object("v",'$.ts').alias("ts"),
    F.get_json_object("v","$.song").alias("song")
).withColumn("timestamp",(F.col("ts").cast("double")/1000).cast("timestamp"))

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

    TOTAL_ACTIVE_LISTENERS.set(int(row["active_user"] or 0 ))
    PAID_USERS_RATIO.set(int(row["paid_users_ratio"] or 0 ))
    AVG_SONG_DURATION.set(int(row["avg_duration"] or 0))
    TOTAL_SONG.set(int(row["total_songs"] or 0))

#Mode append nay nghia la 
windowed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(update_prometheus) \
    .option("checkpointLocation","/opt/spark/checkpoints/streams") \
    .trigger(processingTime="15 seconds") \
    .start()

# Chờ streaming chạy vô hạn (Nếu không có dòng này, script sẽ chạy xong và thoát ngay lập tức)
spark.streams.awaitAnyTermination()