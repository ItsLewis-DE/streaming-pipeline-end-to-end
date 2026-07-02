from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


base_schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", LongType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True)
])

listen_events_schema = StructType(base_schema.fields + [
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True)
])

page_view_events_schema = StructType(base_schema.fields + [
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True)
])

auth_events_schema = StructType(base_schema.fields + [
    StructField("success", StringType(), True)
])

TOPIC_MAP_SCHEMA = {
    "listen_events": listen_events_schema,
    "page_view_events": page_view_events_schema,
    "auth_events": auth_events_schema
}