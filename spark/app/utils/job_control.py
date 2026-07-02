from pyspark.sql import SparkSession, DataFrame 
from pyspark.sql import Functions as F 
import logging 

logger = logging.getLogger(__name__)
def insert_log(spark; SparkSession, bucket_name: str, table_name:str,max_timestamp:str,rundate:str) ->bool:
    try:
        _data = [
            [bucket_name,table_name,max_timestamp,rundate]
        ]
        _cols = ["bucket_name","table_name","max_timestamp","rundate"]

        df_raw = spar.createDataFrame(data = _data,schema = _cols)

        df_processed = df_raw.withColumn("max_timestamp",to_timestamp(lit(max_timestamp))) \
                            .withColumn("rundate",to_timestamp(lit(rundate))) \
                            .withColumn("insert_dt",F.current_timestamp())
        if spark.catalog.tableExists:
            df_proceseed.writeTo("metadata.job_control").mode("append")
        else:
            df_processed.writeTo("metadata.job_control").partitionedBy("insert_dt").create() 
            return True
    except Exception as e:
        logger.error(f"There is an error: {e}")
        return False 