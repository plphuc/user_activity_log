from pyspark.sql import SparkSession

spark = SparkSession.builder\
  .master("local").appName("Python Spark SQL basic example")\
  .config("spark.some.config.option", "some-value").getOrCreate()

def readParquet(paths):
  df=spark.read.parquet(*paths)
  return df

# ----------------C1 [1/1/2024 vs 2/1/2024]----------------
bDayDf = readParquet([f'../2024/01/parquet/1'])
aDayDf = readParquet([f'../2024/01/parquet/2'])

bDayDf.createOrReplaceTempView("bDayDf")
aDayDf.createOrReplaceTempView("aDayDf")

with open("C1.txt", "w") as file:
  bDUsers = spark.sql(f'''SELECT DISTINCT uid \
            FROM bDayDf''')
  numBDUsers = bDUsers.count()
  
  bDUsers.createOrReplaceTempView("bDUsers")
  
  aDUsers = spark.sql(f'''SELECT DISTINCT uid \
            FROM aDayDf''')
  print(aDUsers.count())
  aDUsers.createOrReplaceTempView("aDUsers")
  
  churnUsers = spark.sql(f'''SELECT (*) FROM bDUsers WHERE uid NOT IN (SELECT uid FROM aDUsers)''')
  numChurnUsers = churnUsers.count()
  file.write('Churn User: ' + str(numChurnUsers) + "\n")
  file.write('Churn rate: ' + str(round(numChurnUsers/numBDUsers, 5)) + "\n")

# ------------------- C7-------------------
bWeekDf = readParquet([f'../2024/01/parquet/{i}' for i in range(1, 8)])
aWeekDf = readParquet([f'../2024/01/parquet/{i}' for i in range(8, 15)])

bWeekDf.createOrReplaceTempView("bWeekDf")
aWeekDf.createOrReplaceTempView("aWeekDf")

with open("C7.txt", "w") as file:
  bWUsers = spark.sql(f'''SELECT DISTINCT uid \
            FROM bWeekDf''')
  numBWUsers = bWUsers.count()
  bWUsers.createOrReplaceTempView("bWUsers")
  
  aWUsers = spark.sql(f'''SELECT DISTINCT uid \
            FROM aWeekDf''')
  
  aWUsers.createOrReplaceTempView("aWUsers")

  churnUsers = spark.sql(f'''SELECT COUNT(*) FROM bWUsers WHERE uid NOT IN (SELECT uid FROM aWUsers)''')
  numChurnUsers = churnUsers.count()
  file.write('Churn User: ' + str(numChurnUsers) + "\n")
  file.write('Churn rate: ' + str(round(numChurnUsers/numBWUsers, 2)) + "\n")