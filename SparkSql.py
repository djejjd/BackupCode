from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext

conf = SparkConf().setMaster("local").setAppName("First_App")
spark = SparkSession.builder.appName("app") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path02 = 'hdfs://192.168.191.165:9000/test/Comp_HosRegister.csv'
Comp_HosRegister = spark.read.csv(path02, header=True)


Comp_HosRegister.createOrReplaceTempView("test01")

testDF = spark.sql("SELECT * FROM test01")
testDF.show(1000)