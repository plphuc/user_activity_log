# import pandas as pd
# df = pd.read_csv('2024/01/log23.csv')
# print(df['time'].min())

#--------------------- check parquet file 
#file:///home/plphuc/Desktop/user_log
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
  
df = spark.read.parquet('../test/1')
df.createOrReplaceTempView("parquetFile")
df.show()
print(df.schema)