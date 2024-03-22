from pyspark.sql import SparkSession

spark = SparkSession.builder\
  .master("local").appName("Python Spark SQL basic example")\
  .config("spark.some.config.option", "some-value").getOrCreate()

def readParquet(paths):
  df=spark.read.parquet(*paths)
  return df

bDayDf = readParquet([f'../2024/01/parquet/1'])
aDayDf = readParquet([f'../2024/01/parquet/2'])

bDayDf.createOrReplaceTempView("bDayDf")
aDayDf.createOrReplaceTempView("aDayDf")

with open("N1.txt", "w") as file:
  bDUsers = spark.sql(f'''SELECT DISTINCT uid FROM bDayDf''')
  numBDUsers = bDUsers.count()
  
  bDUsers.createOrReplaceTempView("bDUsers")
  
  aDUsers = spark.sql(f'''SELECT DISTINCT uid FROM aDayDf''')

  aDUsers.createOrReplaceTempView("aDUsers")
  
  newUsers = spark.sql(f'''SELECT uid FROM aDUsers WHERE uid NOT IN (SELECT uid FROM bDUsers)''')
  numChurnUsers = newUsers.count()
  file.write('Number of new users: ' + str(numChurnUsers) + "\n")
