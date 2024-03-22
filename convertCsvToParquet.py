#file:///home/plphuc/Desktop/user_log
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, ByteType, LongType, StructType
from pyspark.sql.functions import from_unixtime

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
  
customSchema = StructType([\
    StructField("uid", IntegerType(), False),\
    StructField("time", LongType(), False),\
    StructField("action", ByteType(), False)])

for i in range (1, 32):
    df = spark.read.option('header', True).csv(f'2024/01/csv/log{i}.csv', schema = customSchema)
    df2 = df.select(df.uid, from_unixtime(df.time, 'dd-MM-yyyy').alias('time'), df.action)
    df2.repartition(10).write.parquet(f"2024/01/parquet/{i}")