from pyspark.sql import SparkSession

spark = SparkSession.builder\
  .master("local").appName("Python Spark SQL basic example")\
  .config("spark.some.config.option", "some-value").getOrCreate()

# READ PARQUET FUNCTION
def readParquet(paths):
  df=spark.read.parquet(*paths)
  return df

#--------------------R1--------------------
fDay = readParquet(['../2024/01/parquet/1'])
sDay = readParquet(['../2024/01/parquet/2'])
tDay = readParquet(['../2024/01/parquet/3'])

fDay.createOrReplaceTempView("fDay")
sDay.createOrReplaceTempView("sDay")
tDay.createOrReplaceTempView("tDay")

userFDay = spark.sql(f'''SELECT DISTINCT uid FROM fDay''')
numUserFDay = userFDay.count()

userSDay = spark.sql(f'''SELECT DISTINCT uid FROM sDay''')
userTDay = spark.sql(f'''SELECT DISTINCT uid FROM tDay''')

userFDay.createOrReplaceTempView("userFDay")
userSDay.createOrReplaceTempView("userSDay")
userTDay.createOrReplaceTempView("userTDay")

numReturnUsers = spark.sql(f'''SELECT (*) FROM userFDay 
                           WHERE uid NOT IN (SELECT uid FROM userSDay)
                           AND uid IN (SELECT uid FROM userTDay)''').count()

with open("R1.txt", "w") as file:
  file.write("Number of returning users: "+ str(numReturnUsers) + "\n")
  file.write("Returning rate: "+ str(round(numReturnUsers/numUserFDay, 5)) + "\n")
  