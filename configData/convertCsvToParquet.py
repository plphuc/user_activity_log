#file:///home/plphuc/Desktop/user_log
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
  
for i in range (1, 32):
    df = spark.read.option('header', True).csv(f'../2024/01/csv/log{i}.csv')
    df.repartition(10).write.parquet(f"../2024/01/parquet/{i}")