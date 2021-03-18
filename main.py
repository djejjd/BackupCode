from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
#
# lines = spark.read.csv("hdfs://localhost:9000/user/hadoop/csv/CFG_DiagnoseTreatItem.csv", encoding="utf-8")
#
# print(lines.count())




spark = SparkSession.builder.appName("local").getOrCreate()
sc = SparkContext.getOrCreate()

lines = sc.textFile("hdfs://localhost:9000/user/hadoop/csv/CFG_DiagnoseTreatItem.csv")

rdd = lines.map(lambda x: x.split(','))
print(rdd.collect())


