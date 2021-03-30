import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


# 打印数据集
def print_Data(path):
    data = spark.read.format('parquet').load(path)
    data.show(10)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "1000") \
        .getOrCreate()

    path_csv = 'hdfs://localhost:9000/data/data_tree'
    print_Data(path_csv)

