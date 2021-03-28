from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


spark = SparkSession.builder \
    .master("local") \
    .appName("example") \
    .config("spark.debug.maxToStringFields", "100") \
    .getOrCreate()

path_csv = 'hdfs://localhost:9000/result/cleaned_form'
path_par = 'hdfs://localhost:9000/result/form_par'
data_csv = spark.read.format('csv').option('header', 'true').load(path_csv)
data_csv.write.format('parquet').mode("overwrite").save(path_par)
