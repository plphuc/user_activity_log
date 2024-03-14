from pyspark.sql import SparkSession

spark = SparkSession.builder\
  .master("local").appName("Python Spark SQL basic example")\
  .config("spark.some.config.option", "some-value").getOrCreate()

# READ PARQUET FUNCTION
def readParquet(paths):
  df=spark.read.parquet(*paths)
  return df
#   #----------------A1----------------
# df = readParquet(['2024/01/parquet/1'])
# df.createOrReplaceTempView("df")
# with open("A1.txt", "w") as file:
#   result = spark.sql("SELECT COUNT(DISTINCT uid) \
#             FROM df").collect()
#   file.write(str(result) + "\n")

#----------------An----------------
n=1 #(A7)
df = readParquet([f'2024/01/parquet/{i}' for i in range(1, n+1)])
# df = readParquet([f'2024/01/parquet/{i}' for i in range(1, n+1)])
df.createOrReplaceTempView("df")
with open("A1.txt", "w") as file:
  result = spark.sql("SELECT COUNT(DISTINCT uid) \
            FROM df").collect()
  file.write(str(result) + "\n")