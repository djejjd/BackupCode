from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local") \
    .appName("example") \
    .config("spark.debug.maxToStringFields", "100") \
    .getOrCreate()

# path_par = 'hdfs://192.168.191.165:9000/zhl/parq'
path_par = 'hdfs://localhost:9000/data/data_tree'

data = spark.read.format('parquet').load(path_par)
data = data.select("HosRegisterCode", "氯化钠注射液", "葡萄糖注射液", "参附注射液").where(data.HosRegisterCode == 'J8030410000002030002')
data.show()

