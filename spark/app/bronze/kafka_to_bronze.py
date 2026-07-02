from pyspark.sql import SparkSession 
from pyspark.sql.functions import current_timestamp, days, from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import sys 
import os

os.environ["PYSPARK_PIN_THREAD"] = "true"

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.schemas import TOPIC_MAP_SCHEMA

spark = SparkSession.builder \
        .appName("KafkaToBronze") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .master("spark://spark-master:7077") \
        .enableHiveSupport() \
        .getOrCreate() 

queries = []

for topic_name, topic_schema in TOPIC_MAP_SCHEMA.items():
    table_name = f"lakehouse.bronze.{topic_name}"
    dlq_name = f"lakehouse.bronze.dlq_{topic_name}"
    
    # Thêm cột _corrupt_record vào schema gốc
    schema_with_corrupt = topic_schema.add("_corrupt_record", StringType(), True)
    
    kafka_df = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka-1:9092") \
                    .option("subscribe", topic_name) \
                    .option("startingOffsets", "earliest") \
                    .load()
    
    # Ép kiểu value và lấy metadata từ Kafka
    df_parsed = kafka_df.selectExpr(
                        "CAST(value AS string) AS raw_string",
                        "partition AS kafka_partition",
                        "offset AS kafka_offset" 
                        ).withColumn(
                            "parsed", 
                            from_json(col("raw_string"), schema_with_corrupt, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"})
                        ).withColumn(
                            "ingested_at", current_timestamp()
                        )
    
    # Bung toàn bộ struct parsed ra ngoài
    final_df = df_parsed.select(
                        "parsed.*",
                        "kafka_partition",
                        "kafka_offset",
                        "ingested_at"
        )
    
    # Sử dụng closure (hàm lồng) để đảm bảo table_name và dlq_name không bị ghi đè trong vòng lặp for
    def get_ingest_func(t_name, d_name):
        def ingest_to_bronze(batch_df, batch_id):
            # Cache (Persist) lại micro-batch để tránh phải đọc/parse lại từ Kafka nhiều lần
            batch_df.persist()
            
            # Tách dữ liệu hợp lệ và dữ liệu lỗi
            valid_df = batch_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            fail_df = batch_df.filter(col("_corrupt_record").isNotNull())
            
            # Ghi dữ liệu hợp lệ vào bảng Bronze
            if not valid_df.isEmpty():
                if spark.catalog.tableExists(t_name):
                    valid_df.writeTo(t_name).append()
                else:
                    valid_df.writeTo(t_name).partitionedBy(days("ingested_at")).create()
            
            # Ghi dữ liệu lỗi vào bảng Dead Letter Queue (DLQ)
            if not fail_df.isEmpty():
                if spark.catalog.tableExists(d_name):
                    fail_df.writeTo(d_name).append()
                else:
                    fail_df.writeTo(d_name).partitionedBy(days("ingested_at")).create()
                    
            # Giải phóng bộ nhớ sau khi xử lý xong batch
            batch_df.unpersist()
            
        return ingest_to_bronze

    # Bắt đầu stream và lưu query
    query = final_df.writeStream \
        .foreachBatch(get_ingest_func(table_name, dlq_name)) \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation", f"/opt/spark/checkpoints/bronze/{topic_name}") \
        .start()
        
    queries.append(query)

# Chờ tất cả các streaming queries chạy vô hạn
spark.streams.awaitAnyTermination()
