import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


conf = SparkConf().setAppName("spark_example")
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

# /csv/Join_Canton.csv/part-00000-7d3b6b4b-d265-4a1a-a759-898a66a0a947-c000.csv
# inputFile = '/home/hadoop/data/test/PersonalInformation.csv'
starttime = time.time()
inputFile01 = 'hdfs://localhost:9000/csv/Join_Canton'
inputs01 = hiveCtx.read.format('csv').option('header', 'true').load(inputFile01)
inputs01.registerTempTable("tweets01")
dataTweets = hiveCtx.sql("""SELECT * from tweets01 where tweets01.Ctn_CantonCode_Ch='150202020601'""")
dataTweets.show()
endtime = time.time()
print(endtime-starttime)


'''
conf = SparkConf().setMaster("local").setAppName("First_App")
spark = SparkSession.builder.appName("app") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path02 = 'hdfs://localhost:9000/csv/test.csv'
Comp_HosRegister = spark.read.csv(path02, header=True)


Comp_HosRegister.createOrReplaceTempView("test01")

testDF = spark.sql("SELECT * FROM test01")
testDF = testDF.toPandas() 
for i, j in zip(testDF["IDCardCode"], testDF["Name"]):
    print(i, j)
'''