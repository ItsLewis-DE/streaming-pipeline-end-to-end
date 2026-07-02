import sys,os

sys.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession 
from pysparl.sql import functions as F 
from datetime import datetime 

spark = SparkSession.builder \
                    .appName("BronzeToSilver") \
                    .config("spark.sql.shuffle.partitions",16) \
                    .master("spark://spark-master:7077") \
                    .enableHiveSupport()
                    .getOrCreate()

