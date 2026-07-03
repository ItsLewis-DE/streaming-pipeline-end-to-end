import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.job_control import insert_log, get_max_timestamp
from utils.schemas import ALL_TOPICS
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from datetime import datetime 

spark = SparkSession.builder \
                    .appName("BronzeToSilver") \
                    .config("spark.sql.shuffle.partitions", 16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()

bucket_name = "bronze"
for topic_name in ALL_TOPICS:
    max_timestamp = get_max_timestamp(spark, bucket_name, topic_name)
    df = spark.table(f"lakehouse.{bucket_name}.{topic_name}") \
              .where(f"ingested_at > '{max_timestamp}'")

    # deduplicate
    window_spec = Window.partitionBy("kafka_partition", "kafka_offset").orderBy(F.col("ingested_at").desc())
    df_dedup = df.withColumn("row_num", F.row_number().over(window_spec)) \
                 .withColumn("_from_bucket", F.lit(topic_name)) \
                 .where(F.col("row_num") == 1).drop("row_num")
                 
    df_processed = df_dedup.withColumn(
        'registration', F.to_timestamp(F.col('registration').cast('long')/1000)
    ).withColumn(
        'ts', F.to_timestamp(F.col('ts').cast('long')/1000)
    )
    
    target_table = f"lakehouse.silver.{topic_name}"
    if not df_processed.isEmpty():
        # Get the new max_timestamp of this batch
        max_ts_row = df_processed.agg(F.max("ingested_at").alias("max_ts")).collect()[0]
        new_max_ts = max_ts_row["max_ts"]
        
        if spark.catalog.tableExists(target_table):
            df_processed.writeTo(target_table).append()
        else:
            df_processed.writeTo(target_table).partitionedBy(F.days("ingested_at")).create()            
            
        if new_max_ts:
            insert_log(spark, "bronze", topic_name, str(new_max_ts))