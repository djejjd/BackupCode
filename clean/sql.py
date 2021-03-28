# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
# from pyspark.sql.functions import *
#
#
# spark = SparkSession.builder \
#     .master("local") \
#     .appName("example") \
#     .config("spark.debug.maxToStringFields", "100") \
#     .getOrCreate()
#
# inputFile01 = 'hdfs://localhost:9000/result/form_par'
# # inputFile01 = 'hdfs://localhost:9000/data/data_tree'
# # inputFile01 = '/home/hadoop/data/test/csv'
# inputs01 = spark.read.format('parquet').load(inputFile01)
# inputs01.createOrReplaceTempView("tweets01")
# dataTweets = spark.sql("""SELECT Name, HosRegisterCode, DrugName from tweets01 where tweets01.Name = '柳三女'""")
# dataTweets.show(100)


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('JupyterPySpark').enableHiveSupport().getOrCreate()
import pyspark.sql.functions as F

# 原始数据
test = spark.createDataFrame([('2018-01', '项目1', 100), ('2018-01', '项目2', 200), ('2018-01', '项目3', 300),
                              ('2018-02', '项目1', 1000), ('2018-02', '项目2', 2000), ('2018-03', '项目4', 999),
                              ('2018-05', '项目1', 6000), ('2018-05', '项目2', 4000), ('2018-05', '项目4', 1999)
                              ], ['月份', '项目', '收入'])
test.show()

test_pivot = test.groupBy('月份') \
    .pivot('项目', ['项目1', '项目2', '项目3', '项目4']) \
    .agg(F.sum('收入')) \
    .fillna(0)

test_pivot.show()
