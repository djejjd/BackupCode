import pandas as pd
import pymysql
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

'''
part-00002-f040da2a-5abd-4025-a8ba-5b2c48a1531a-c000.csv
'''


def get_new_par_drug():
    path = '/home/hadoop/data/csv'
    df = spark.read.format('csv').option('header', 'true').load(path)
    path1 = 'hdfs://localhost:9000/result/drugNotesPar'
    df.write.format('parquet').mode("overwrite").save(path1)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

    # get_new_par_drug()

