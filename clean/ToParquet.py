from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


conf = SparkConf().setAppName("spark_example")
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

path_csv = 'hdfs://localhost:9000/result/cleaned_form'
path_par = 'hdfs://localhost:9000/result/form_par'
data_csv = hiveCtx.read.format('csv').option('header', 'true').load(path_csv)
data_csv.limit(50).write.format('parquet').save(path_par)
