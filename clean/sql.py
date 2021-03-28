from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
from pyspark.sql.functions import *


conf = SparkConf().setAppName("spark_example").set("spark.default.parallelism", '16')
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

# inputFile01 = 'hdfs://localhost:9000/result/form_par'
# inputs01 = hiveCtx.read.format('parquet').load(inputFile01)
# inputs01.registerTempTable("tweets01")
# dataTweets = hiveCtx.sql("""SELECT CertificateCode, Name, Sex, HosRegisterCode from tweets01 where tweets01.Name='柳三女'""")
# dataTweets.show(1000)