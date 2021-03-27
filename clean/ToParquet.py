from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row

conf = SparkConf().setAppName("spark_example").set("spark.default.parallelism", '16')
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

inputFile01 = 'hdfs://localhost:9000/result/all_form'
inputFile02 = 'hdfs://localhost:9000/result/form_par'
inputs01 = hiveCtx.read.format('csv').option('header', 'true').load(inputFile01)
inputs01.write.format('parquet').save(inputFile02)