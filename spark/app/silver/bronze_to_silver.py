import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.job_control import insert_log, get_max_timestamp
from utils.schemas import ALL_TOPICS
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from datetime import datetime 
import uuid

spark = SparkSession.builder \
                    .appName("BronzeToSilver") \
                    .config("spark.sql.shuffle.partitions", 16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport() \
                    .getOrCreate()

bucket_name = "bronze"
#Tạo pipelineID cho mỗi lần xử lí
my_uuid = str(uuid.uuid4())
for topic_name in ALL_TOPICS:
    try:
        max_timestamp = get_max_timestamp(spark, bucket_name, topic_name)
        
        source_table = f"lakehouse.{bucket_name}.{topic_name}"
        if not spark.catalog.tableExists(source_table):
            print(f"Bảng {source_table} chưa tồn tại, bỏ qua.")
            continue
            
        df = spark.table(source_table).where(f"ingested_at > '{max_timestamp}'")

        if df.isEmpty():
            continue

        # deduplicate
        window_spec = Window.partitionBy("kafka_partition", "kafka_offset").orderBy(F.col("ingested_at").desc())
        df_dedup = df.withColumn("row_num", F.row_number().over(window_spec)) \
                     .withColumn("_from_bucket", F.lit(topic_name)) \
                     .where(F.col("row_num") == 1).drop("row_num")
        # Extract PII Mapping
        pii_cols = ["userId", "firstName", "lastName", "zip", "lon", "lat"]
        if all(c in df_dedup.columns for c in pii_cols):
            df_pii = df_dedup.filter(F.col("userId").isNotNull()).select(*pii_cols).dropDuplicates(["userId"])
            df_pii = df_pii.withColumn("user_pseudo_id", F.sha2(F.concat_ws("", F.col("userId").cast("string"), F.lit("super_secret_music_salt")), 256)) #Them salt là để tránh hacker dùng Rainbow Table để truy lại dữ liệu của người dùng.
            df_pii = df_pii.withColumn("updated_at", F.current_timestamp())
            
            pii_table = "lakehouse.silver.pii_mapping"
            if spark.catalog.tableExists(pii_table):
                df_pii.writeTo(pii_table).option("mergeSchema", "true").append()
            else:
                df_pii.writeTo(pii_table).partitionedBy(F.days("updated_at")).create()
                     
        df_processed = df_dedup.withColumn(
            'registration', F.to_timestamp(F.col('registration').cast('long')/1000)
        ).withColumn(
            'ts', F.to_timestamp(F.col('ts').cast('long')/1000)
        ).withColumn(
            '_processed_by', F.lit('bronze_to_silver_v1.0')
        ).withColumn(
            '_processed_at', F.current_timestamp()
        ).withColumn(
            '_pipeline_run_id', F.lit(my_uuid)
        )

        cols_to_drop = []
        # Masking PII fields
        if "userId" in df_processed.columns:
            df_processed = df_processed.withColumn(
                'user_pseudo_id', F.when(F.col('userId').isNotNull(), F.sha2(F.concat_ws("", F.col("userId").cast("string"), F.lit("super_secret_music_salt")), 256)).otherwise(F.lit(None))
            )
        if "zip" in df_processed.columns:
            df_processed = df_processed.withColumn(
                'zip', F.when(F.col('zip').isNotNull(), F.concat(F.substring(F.col('zip'), 1, 3), F.lit("**"))).otherwise(F.lit(None))
            )
        if "lon" in df_processed.columns:
            df_processed = df_processed.withColumn('lon', F.round(F.col('lon'), 1))
        if "lat" in df_processed.columns:
            df_processed = df_processed.withColumn('lat', F.round(F.col('lat'), 1))
            
        cols_to_drop = [c for c in ["userId", "firstName", "lastName"] if c in df_processed.columns]
        if cols_to_drop:
            df_processed = df_processed.drop(*cols_to_drop)
        
        target_table = f"lakehouse.silver.{topic_name}"
        
        stats = df_processed.agg(
            F.min("ingested_at").alias("min_ts"),
            F.max("ingested_at").alias("max_ts"),
            F.count("*").alias("row_count")
        ).collect()[0]
        
        new_min_ts = max_timestamp
        new_max_ts = stats["max_ts"]
        row_count = stats["row_count"]
        
        if row_count > 0:
            if spark.catalog.tableExists(target_table):
                df_processed.writeTo(target_table).option("mergeSchema", "true").append()
            else:
                df_processed.writeTo(target_table).partitionedBy(F.days("_updated_at")).create()            
                
            insert_log(
                spark=spark, 
                bucket_name=bucket_name, 
                table_name=topic_name, 
                min_timestamp=str(new_min_ts), 
                max_timestamp=str(new_max_ts), 
                row_count=row_count, 
                status="SUCCESS"
            )
            
    except Exception as e:
        insert_log(
            spark=spark, 
            bucket_name=bucket_name, 
            table_name=topic_name, 
            min_timestamp="", 
            max_timestamp="", 
            row_count=0, 
            status="FAILED",
            error_message=str(e)
        )
        raise e