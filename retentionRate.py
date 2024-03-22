from pyspark.sql import SparkSession

spark = SparkSession.builder\
  .master("local").appName("Python Spark SQL basic example")\
  .config("spark.some.config.option", "some-value").getOrCreate()

# READ PARQUET FUNCTION
def readParquet(paths):
  df=spark.read.parquet(*paths)
  return df

# ----------------- retention rate in 7 days from 10/1 - 17/1 ------------------
CS = readParquet([f'2024/01/parquet/{i}' for i in range(1, 10)])
CS.createOrReplaceTempView("CS")
CE = readParquet([f'2024/01/parquet/{i}' for i in range(10, 18)])
CE.createOrReplaceTempView("CE")

CSUsers = spark.sql(f'''SELECT DISTINCT uid FROM CS''')

CSUsers.createOrReplaceTempView("CSUsers")

retentionUsers = spark.sql(f'''SELECT uid FROM CSUsers WHERE uid IN
                           (SELECT DISTINCT uid FROM CE)''')
retentionUsers.show()
with open('retentionRate.txt', 'w') as f:
  f.write(f'Retention users:{retentionUsers.count()} \n')
  f.write(f'Retention rate: {round(retentionUsers.count()/CSUsers.count(), 2)}')