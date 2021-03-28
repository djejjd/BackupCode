from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


# df.withColumn('name', F.split(F.col('name'), '')).show()

def create_data(path):
    data_par = spark.read.format('parquet').load(path)
    data = data_par.select('HosRegisterCode', 'DrugName').where(data_par['DrugName'] != '0')
    data = data.withColumn('Times', lit(1))
    data_pivot = data.groupBy('HosRegisterCode').pivot('DrugName').agg(sum('Times')).fillna(0)
    data_pivot.show(100)


if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local") \
            .appName("example") \
            .config("spark.debug.maxToStringFields", "100")\
            .getOrCreate()

    # path_par = 'hdfs://192.168.191.165:9000/zhl/parq'
    path_par = 'hdfs://localhost:9000/result/form_par'

    create_data(path_par)
